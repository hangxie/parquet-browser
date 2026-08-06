package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gorilla/mux"
	pio "github.com/hangxie/parquet-tools/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_ConcurrentReaderAccess overlaps requests against the single shared
// reader, which would race without serialization. Half the workers cancel
// mid-flight to exercise cross-request cancellation. Run under -race.
func Test_ConcurrentReaderAccess(t *testing.T) {
	svc, err := NewParquetService(context.Background(), getTestParquetFile(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to create service: %v", err)
	}
	defer func() { _ = svc.Close() }()

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	// Endpoints spanning schema, page-metadata, and page-content reads — the paths
	// touching the shared reader's IO. Keep those returning 200 for this fixture
	// and record single-threaded bodies as a baseline for corruption under load.
	candidates := []string{
		"/schema/json",
		"/rowgroups/0/columnchunks/0/pages",
		"/rowgroups/0/columnchunks/0/pages/0",
		"/rowgroups/0/columnchunks/0/pages/0/content",
	}
	baseline := map[string]string{}
	var endpoints []string
	for _, ep := range candidates {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, ep, nil))
		if w.Code == http.StatusOK {
			baseline[ep] = w.Body.String()
			endpoints = append(endpoints, ep)
		}
	}
	require.GreaterOrEqual(t, len(endpoints), 2, "need at least two reader-backed endpoints to exercise concurrency")

	const workers = 32
	const iterations = 8

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cancelling := i%2 == 0
			for j := 0; j < iterations; j++ {
				ep := endpoints[(i+j)%len(endpoints)]

				ctx, cancel := context.WithCancel(context.Background())
				req := httptest.NewRequest(http.MethodGet, ep, nil).WithContext(ctx)
				if cancelling {
					// Race the cancellation against the in-flight read.
					go cancel()
				}

				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)

				// Requests that never cancel must always observe correct,
				// uncorrupted data regardless of what concurrent requests do to
				// the shared reader.
				if !cancelling {
					assert.Equal(t, http.StatusOK, w.Code, ep)
					assert.Equal(t, baseline[ep], w.Body.String(), ep)
				}
				cancel()
			}
		}(i)
	}
	wg.Wait()
}

// Test_CancelledRequest_NotReportedAsNotFound verifies that with an already
// cancelled request context, reader-backed handlers report a client-cancelled
// status rather than a 404 or a generic 500 for schema.
func Test_CancelledRequest_NotReportedAsNotFound(t *testing.T) {
	svc, err := NewParquetService(context.Background(), getTestParquetFile(), pio.ReadOption{})
	if err != nil {
		t.Skipf("Failed to create service: %v", err)
	}
	defer func() { _ = svc.Close() }()

	router := mux.NewRouter()
	svc.SetupRoutes(router)

	endpoints := []string{
		"/schema/json",
		"/rowgroups/0/columnchunks/0/pages",
		"/rowgroups/0/columnchunks/0/pages/0",
		"/rowgroups/0/columnchunks/0/pages/0/content",
	}
	for _, ep := range endpoints {
		t.Run(ep, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // already cancelled before the handler runs
			req := httptest.NewRequest(http.MethodGet, ep, nil).WithContext(ctx)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			require.Equal(t, StatusClientClosedRequest, w.Code,
				"cancelled request should report client-closed, not %d", w.Code)
			require.NotEqual(t, http.StatusNotFound, w.Code)
		})
	}
}
