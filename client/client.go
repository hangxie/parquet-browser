package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/hangxie/parquet-browser/model"
)

// ParquetClient is an HTTP client for accessing parquet data
type ParquetClient struct {
	baseURL string
	client  *http.Client
}

// NewParquetClient creates a new HTTP client
func NewParquetClient(baseURL string) *ParquetClient {
	return &ParquetClient{
		baseURL: baseURL,
		client:  &http.Client{},
	}
}

// GetFileInfo retrieves file-level metadata
func (c *ParquetClient) GetFileInfo() (model.FileInfo, error) {
	var info model.FileInfo
	err := c.get("/info", &info)
	return info, err
}

// GetAllRowGroupsInfo retrieves all row groups
func (c *ParquetClient) GetAllRowGroupsInfo() ([]model.RowGroupInfo, error) {
	var rowGroups []model.RowGroupInfo
	err := c.get("/rowgroups", &rowGroups)
	return rowGroups, err
}

// GetRowGroupInfo retrieves info for a specific row group
func (c *ParquetClient) GetRowGroupInfo(rgIndex int) (model.RowGroupInfo, error) {
	var info model.RowGroupInfo
	err := c.get(fmt.Sprintf("/rowgroups/%d", rgIndex), &info)
	return info, err
}

// GetAllColumnChunksInfo retrieves all column chunks for a row group
func (c *ParquetClient) GetAllColumnChunksInfo(rgIndex int) ([]model.ColumnChunkInfo, error) {
	var columns []model.ColumnChunkInfo
	err := c.get(fmt.Sprintf("/rowgroups/%d/columnchunks", rgIndex), &columns)
	return columns, err
}

// GetColumnChunkInfo retrieves info for a specific column chunk
func (c *ParquetClient) GetColumnChunkInfo(rgIndex, colIndex int) (model.ColumnChunkInfo, error) {
	var info model.ColumnChunkInfo
	err := c.get(fmt.Sprintf("/rowgroups/%d/columnchunks/%d", rgIndex, colIndex), &info)
	return info, err
}

// GetAllPagesInfo retrieves all page metadata for a column chunk
func (c *ParquetClient) GetAllPagesInfo(rgIndex, colIndex int) ([]model.PageMetadata, error) {
	var pages []model.PageMetadata
	err := c.get(fmt.Sprintf("/rowgroups/%d/columnchunks/%d/pages", rgIndex, colIndex), &pages)
	return pages, err
}

// GetPageInfo retrieves info for a specific page
func (c *ParquetClient) GetPageInfo(rgIndex, colIndex, pageIndex int) (model.PageMetadata, error) {
	var info model.PageMetadata
	err := c.get(fmt.Sprintf("/rowgroups/%d/columnchunks/%d/pages/%d", rgIndex, colIndex, pageIndex), &info)
	return info, err
}

// GetPageContent retrieves the pre-formatted values/content of a specific page
// Values are returned as strings, already formatted for display
func (c *ParquetClient) GetPageContent(rgIndex, colIndex, pageIndex int) ([]string, error) {
	var response struct {
		Values []string `json:"values"`
		Count  int      `json:"count"`
	}
	err := c.get(fmt.Sprintf("/rowgroups/%d/columnchunks/%d/pages/%d/content", rgIndex, colIndex, pageIndex), &response)
	return response.Values, err
}

// GetSchemaGo retrieves the schema in Go struct format
func (c *ParquetClient) GetSchemaGo() (string, error) {
	return c.getText("/schema/go")
}

// GetSchemaJSON retrieves the schema in JSON format (compact)
func (c *ParquetClient) GetSchemaJSON() (string, error) {
	return c.getText("/schema/json")
}

// GetSchemaRaw retrieves the raw schema tree structure (compact JSON)
func (c *ParquetClient) GetSchemaRaw() (string, error) {
	return c.getText("/schema/raw")
}

// GetSchemaCSV retrieves the schema in CSV format
func (c *ParquetClient) GetSchemaCSV() (string, error) {
	return c.getText("/schema/csv")
}

// Helper method to make GET requests and decode JSON
func (c *ParquetClient) get(path string, result interface{}) error {
	url := c.baseURL + path

	resp, err := c.client.Get(url)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// Try to read error message from response
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

// Helper method to make GET requests and return text
func (c *ParquetClient) getText(path string) (string, error) {
	url := c.baseURL + path

	resp, err := c.client.Get(url)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		// Try to read error message from response
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	return string(body), nil
}
