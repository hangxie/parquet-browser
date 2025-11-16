package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/hangxie/parquet-browser/model"
)

// parquetClient is an HTTP client for accessing parquet data
type parquetClient struct {
	baseURL string
	client  *http.Client
}

// newParquetClient creates a new HTTP client
func newParquetClient(baseURL string) *parquetClient {
	return &parquetClient{
		baseURL: baseURL,
		client:  &http.Client{},
	}
}

// getFileInfo retrieves file-level metadata
func (c *parquetClient) getFileInfo() (model.FileInfo, error) {
	var info model.FileInfo
	err := c.get("/info", &info)
	return info, err
}

// getAllRowGroupsInfo retrieves all row groups
func (c *parquetClient) getAllRowGroupsInfo() ([]model.RowGroupInfo, error) {
	var rowGroups []model.RowGroupInfo
	err := c.get("/rowgroups", &rowGroups)
	return rowGroups, err
}

// getRowGroupInfo retrieves info for a specific row group
func (c *parquetClient) getRowGroupInfo(rgIndex int) (model.RowGroupInfo, error) {
	var info model.RowGroupInfo
	err := c.get(fmt.Sprintf("/rowgroups/%d", rgIndex), &info)
	return info, err
}

// getAllColumnChunksInfo retrieves all column chunks for a row group
func (c *parquetClient) getAllColumnChunksInfo(rgIndex int) ([]model.ColumnChunkInfo, error) {
	var columns []model.ColumnChunkInfo
	err := c.get(fmt.Sprintf("/rowgroups/%d/columnchunks", rgIndex), &columns)
	return columns, err
}

// getColumnChunkInfo retrieves info for a specific column chunk
func (c *parquetClient) getColumnChunkInfo(rgIndex, colIndex int) (model.ColumnChunkInfo, error) {
	var info model.ColumnChunkInfo
	err := c.get(fmt.Sprintf("/rowgroups/%d/columnchunks/%d", rgIndex, colIndex), &info)
	return info, err
}

// getAllPagesInfo retrieves all page metadata for a column chunk
func (c *parquetClient) getAllPagesInfo(rgIndex, colIndex int) ([]model.PageMetadata, error) {
	var pages []model.PageMetadata
	err := c.get(fmt.Sprintf("/rowgroups/%d/columnchunks/%d/pages", rgIndex, colIndex), &pages)
	return pages, err
}

// getPageInfo retrieves info for a specific page
func (c *parquetClient) getPageInfo(rgIndex, colIndex, pageIndex int) (model.PageMetadata, error) {
	var info model.PageMetadata
	err := c.get(fmt.Sprintf("/rowgroups/%d/columnchunks/%d/pages/%d", rgIndex, colIndex, pageIndex), &info)
	return info, err
}

// getPageContent retrieves the pre-formatted values/content of a specific page
// Values are returned as strings, already formatted for display
func (c *parquetClient) getPageContent(rgIndex, colIndex, pageIndex int) ([]string, error) {
	var response struct {
		Values []string `json:"values"`
		Count  int      `json:"count"`
	}
	err := c.get(fmt.Sprintf("/rowgroups/%d/columnchunks/%d/pages/%d/content", rgIndex, colIndex, pageIndex), &response)
	return response.Values, err
}

// getSchemaGo retrieves the schema in Go struct format
func (c *parquetClient) getSchemaGo() (string, error) {
	return c.getText("/schema/go")
}

// getSchemaJSON retrieves the schema in JSON format (compact)
func (c *parquetClient) getSchemaJSON() (string, error) {
	return c.getText("/schema/json")
}

// getSchemaRaw retrieves the raw schema tree structure (compact JSON)
func (c *parquetClient) getSchemaRaw() (string, error) {
	return c.getText("/schema/raw")
}

// getSchemaCSV retrieves the schema in CSV format
func (c *parquetClient) getSchemaCSV() (string, error) {
	return c.getText("/schema/csv")
}

// Helper method to make GET requests and decode JSON
func (c *parquetClient) get(path string, result interface{}) error {
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
func (c *parquetClient) getText(path string) (string, error) {
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
