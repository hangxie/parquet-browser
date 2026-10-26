package service

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strings"

	preader "github.com/hangxie/parquet-go/v3/reader"
)

// webErrorData is the data model for the "error" template partial.
type webErrorData struct {
	Title   string
	Message string
	Detail  string
}

func formatRatio(ratio float64) string {
	if ratio == 0 {
		return "N/A"
	}
	return fmt.Sprintf("%.2fx", ratio)
}

// renderPartial renders a template partial, wrapping it in full HTML if not an HTMX request
func renderPartial(w http.ResponseWriter, r *http.Request, templateName string, data interface{}) error {
	isHTMX := r.Header.Get("HX-Request") == "true"
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if isHTMX {
		return templates.ExecuteTemplate(w, templateName, data)
	}

	// Direct browser requests get the partial wrapped in the full page.
	var buf strings.Builder
	if err := templates.ExecuteTemplate(&buf, templateName, data); err != nil {
		return err
	}

	wrapperData := struct {
		Content template.HTML
		Version string
	}{
		Content: template.HTML(buf.String()),
		Version: getVersion(),
	}

	return templates.ExecuteTemplate(w, "wrapper.html", wrapperData)
}

// renderPagesError shows a reader error as a modal over the current view.
func renderPagesError(w http.ResponseWriter, r *http.Request, err error) {
	title, message := describeReaderError(err)
	data := webErrorData{
		Title:   title,
		Message: message,
		Detail:  err.Error(),
	}
	// HTMX only swaps on 2xx, so retarget to <body> and keep the status 200.
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Retarget", "body")
		w.Header().Set("HX-Reswap", "beforeend")
		w.Header().Set("HX-Push-Url", "false")
	} else {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(contextErrorStatus(err, http.StatusNotFound))
	}
	if rerr := renderPartial(w, r, "error", data); rerr != nil {
		http.Error(w, rerr.Error(), http.StatusInternalServerError)
	}
}

// describeReaderError turns a reader error into a user-facing title and message.
func describeReaderError(err error) (title, message string) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "Request interrupted", "The request was cancelled before it completed."
	}
	// Name the flag to pass when a column needs a key.
	if errors.Is(err, preader.ErrColumnKeyRequired) {
		columnPath := columnPathFromKeyRequiredError(err)
		if columnPath != "" {
			return "Column requires a decryption key",
				fmt.Sprintf("Column %s is encrypted. Restart the server with --column-key=%s=<base64-key> (or --key-file=<path>) to read it.", columnPath, columnPath)
		}
		return "Column requires a decryption key",
			"This column is encrypted. Restart the server with --column-key=<column>=<base64-key> (or --key-file=<path>) to read it."
	}
	return "Cannot read this column", err.Error()
}

func columnPathFromKeyRequiredError(err error) string {
	const marker = " for column "
	msg := err.Error()
	i := strings.LastIndex(msg, marker)
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(msg[i+len(marker):])
}

func formatNullCount(count *int64) string {
	if count == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *count)
}
