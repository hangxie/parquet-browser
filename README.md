# parquet-browser

A triple-mode tool for browsing and inspecting Apache Parquet files:
- **TUI Mode**: Interactive Terminal User Interface for file browsing
- **Server Mode**: HTTP API server for programmatic access
- **Web UI Mode**: Modern web-based interface with HTMX for interactive browsing

This tool is built upon [`github.com/hangxie/parquet-tools`](https://github.com/hangxie/parquet-tools) and [`github.com/hangxie/parquet-go`](https://github.com/hangxie/parquet-go). For batch processing or command-line operations on Parquet files, it is recommended to use `parquet-tools` directly.

## Backend Support

`parquet-browser` supports reading files from various storage systems, including:

- Local file system
- HTTP/HTTPS
- HDFS
- S3
- Google Cloud Storage (GCS)
- Azure Blob Storage

For URLs for different object store and access options, please refer to [Parquet File Location](https://github.com/hangxie/parquet-tools?tab=readme-ov-file#parquet-file-location).

## Features

### Triple-Mode Operation
- **TUI Mode**: Interactive terminal interface with embedded HTTP server
- **Server Mode**: Standalone HTTP API server for programmatic access
- **Web UI Mode**: Modern browser-based interface with HTMX for dynamic updates
- **HTTP API**: Complete RESTful API with JSON responses
- **OpenAPI Documentation**: Swagger/OpenAPI 3.0 specification included ([swagger.yaml](swagger.yaml))

### TUI Features
- **Interactive File Browser**: Open parquet files through command line with loading progress and cancellation support (ESC or Ctrl+C)
- **File Summary View**: Quick overview of file metadata:
  - Version, row groups, total rows, leaf columns
  - Total compressed and uncompressed sizes with compression ratio
  - Created by information
- **Schema Viewer**: View schema in multiple formats (JSON, Raw, Go Struct, CSV) with:
  - Toggle between formats with 'v' key
  - Pretty/compact mode toggle with 'p' key (JSON and Raw formats)
  - Copy to clipboard with 'c' key
  - Support for complex types (LIST, MAP, STRUCT)
- **Row Group Explorer**: Browse row groups with detailed information:
  - Row count per group
  - Number of columns
  - Total values and total nulls across all columns
  - Size display: compressed → uncompressed (ratio)
  - Easy navigation with arrow keys
  - Press Enter to view column chunks
- **Column Chunk Inspector**: Deep dive into column storage:
  - Column path (max 30 chars for better layout), physical type, logical type, converted type
  - Compression codec
  - Number of values and null count
  - Size: compressed → uncompressed (ratio)
  - Min/Max statistics for data distribution analysis
  - Press Enter to view page-level details
- **Page-Level Details**: Inspect internal page structure:
  - View all pages (DATA_PAGE, DATA_PAGE_V2, DICTIONARY_PAGE, INDEX_PAGE)
  - Page type (max 15 chars for better layout), offsets, compressed/uncompressed sizes
  - Number of values, encoding information
  - Min/Max statistics per page for data distribution analysis
  - Null count per page
  - Press Enter to view actual page content
- **Page Content Viewer**: Browse decoded page values:
  - Complete page metadata header (type, offset, size, values count, encoding)
  - Display all values from a single page
  - Smart value formatting (UTF-8 strings, hex for binary data)
  - Row numbers for reference
  - Handles NULL values explicitly
- **Type-Aware Display**: Proper handling of complex Parquet types (LIST, MAP, STRUCT, DECIMAL, TIMESTAMP, etc.)
- **Error Handling**: Graceful error handling with cancellable loading operations
- **Keyboard Navigation**: Full keyboard support for efficient browsing

### Web UI Features
- **Modern Browser Interface**: Clean, responsive web interface with HTMX for dynamic updates
- **File Overview**: Home page displays file metadata and all row groups at once
- **Schema Viewer**: View schema in multiple formats (Go, JSON, Raw, CSV) with syntax highlighting
- **Row Group Browser**: Navigate through row groups with detailed statistics
  - Total values and total nulls per row group
  - Size information: compressed → uncompressed (ratio)
- **Column Chunk Inspector**: Explore column storage with complete metadata
  - Min/Max statistics for each column chunk
  - Type information (physical, logical, converted)
  - Compression codec and size details
- **Page Inspector**: View page-level details for column chunks
  - Complete column chunk metadata in header
  - Min/Max statistics for each page
  - Page type, offset, encoding, and size information
- **Page Content Viewer**: Browse actual data values
  - Complete page metadata header
  - All decoded values from the page
  - Smart formatting for different data types
- **Breadcrumb Navigation**: Easy navigation back to any level
- **No JavaScript Required**: Progressive enhancement with HTMX

## Installation

```bash
go build -o parquet-browser
```

Or install directly:

```bash
go install github.com/hangxie/parquet-browser@latest
```

## Usage

### TUI Mode (Interactive Browser)

Open a file directly in the TUI:

```bash
./parquet-browser tui path/to/file.parquet
```

The application will show a loading modal and then display the file contents. Press ESC during loading to cancel.

**How it works:**
- Starts an embedded HTTP server on a random localhost port
- Server runs in "quiet mode" (no console logging)
- TUI communicates with the server via HTTP
- Server shuts down automatically when TUI exits

### Server Mode (HTTP API)

Run as a standalone HTTP API server:

```bash
# Start server on default port (:8080)
./parquet-browser serve file.parquet

# Start server on custom port
./parquet-browser serve -a :9090 file.parquet
```

The server provides RESTful endpoints for programmatic access. See [API Documentation](#http-api) below.

### Web UI Mode

Run the web-based interface:

```bash
# Start web UI on default port (:8080)
./parquet-browser webui file.parquet

# Start web UI on custom port
./parquet-browser webui -a :9090 file.parquet
```

The web UI will automatically open in your default browser. Navigate through file metadata, row groups, column chunks, pages, and view actual data values in a modern web interface.

### Open Remote Files

Works in all three modes (TUI, server, and web UI):

```bash
# S3
./parquet-browser tui s3://bucket/path/to/file.parquet
./parquet-browser serve s3://bucket/path/to/file.parquet

# HTTP/HTTPS
./parquet-browser tui https://example.com/data.parquet
./parquet-browser serve https://example.com/data.parquet

# With additional options
./parquet-browser tui --anonymous wasbs://laborstatisticscontainer@azureopendatastorage.blob.core.windows.net/lfs/part-00000-tid-6312913918496818658-3a88e4f5-ebeb-4691-bfb6-e7bd5d4f2dd0-63558-c000.snappy.parquet
./parquet-browser tui --http-ignore-tls-error https://example.com/file.parquet
```

### Help

```bash
./parquet-browser --help
```

Display usage information and available flags.

### Keyboard Shortcuts

#### Main View (Row Groups)
- `↑` / `↓`: Navigate through row groups
- `Enter`: View column chunks for selected row group
- `s`: Show schema viewer
- `q` / `Esc`: Quit application

#### Schema Viewer
- `v`: Toggle between schema formats (JSON → Raw → Go Struct → CSV)
- `p`: Toggle pretty/compact mode (JSON and Raw only)
- `c`: Copy schema to clipboard
- `Esc`: Close schema viewer

#### Column Chunks View
- `↑` / `↓`: Navigate through column chunks
- `Enter`: View page-level details for selected column chunk
- `Esc`: Close column chunks view

#### Page Details View
- `↑` / `↓`: Navigate through pages
- `Enter`: View page content (all decoded values)
- `Esc`: Close page details view

#### Page Content View
- `↑` / `↓`: Navigate through values
- `Esc`: Close page content view

#### Loading Modals
- `Esc` / `Ctrl+C`: Cancel loading operation

## Interface Layout

### Main Screen

```
┌─ File Info ───────────────────────────────────────────────────────────────┐
│ File: data.parquet                                                        │
│ Version: 1  Row Groups: 1  Rows: 3  Columns: 71                           │
│ Total Size: 12.6 MB → 22.2 MB (1.76x)  Created By: parquet-go version ... │
└───────────────────────────────────────────────────────────────────────────┘
┌─ Row Groups (↑↓ to navigate) ─────────────────────────────────────────────┐
│  #  │   Rows   │ Columns │              Size               │              │
│  0  │     3    │   71    │   12.6 MB → 22.2 MB (1.76x)     │              │
└───────────────────────────────────────────────────────────────────────────┘
 Keys: ESC=quit, s=schema, ↑↓=scroll, Enter=see item details
```

Features:
- **File Info**: File name on top, followed by version, row groups, total rows, leaf columns count
- **Total Size**: Shows compressed → uncompressed size with compression ratio
- **Row Groups**: Lists all row groups with their metadata
- **Status Line**: Keyboard shortcuts only (ESC, s, arrows, Enter)
- **Press Enter**: View column chunks for selected row group

### Column Chunks View

Press `Enter` on a row group:

```
┌─ Row Group Info ──────────────────────────────────────────────────────────┐
│ Row Group: 0  Rows: 3  Columns: 71                                        │
│ Total Values: 213  Total Nulls: 0                                         │
│ Size: 12.6 MB → 22.2 MB (1.76x)                                           │
└───────────────────────────────────────────────────────────────────────────┘
┌─ Column Chunks (↑↓ to navigate, Enter=view pages) ────────────────────────┐
│  #  │ Name              │ Type       │ Codec  │ Size        │ Min  │ Max  │
│  0  │ doc.id            │ INT64      │ SNAPPY │ 45 B → 51 B │ 1    │ 3    │
│  1  │ doc.title         │ BYTE_ARRAY │ SNAPPY │ 85 B → 91 B │ "A"  │ "Z"  │
│  2  │ doc.dataProvider  │ BYTE_ARRAY │ SNAPPY │ 165 B → 233 │ "AA" │ "ZZ" │
│ ... │       ...         │    ...     │  ...   │     ...     │ ...  │ ...  │
└───────────────────────────────────────────────────────────────────────────┘
 Keys: ESC=back, s=schema, ↑↓=scroll, Enter=see item details
```

Features:
- **Row Group Info**: Row group number, rows, columns, total values/nulls, size (3-line header)
- **Column details**: Name (max 30 chars), type, codec, sizes, and Min/Max statistics
- **Status Line**: Consistent keyboard shortcuts across all views
- **Press Enter**: View page-level details

### Page Details View

Press `Enter` on a column chunk:

```
┌─ Column Chunk Info ───────────────────────────────────────────────────────┐
│ Column: doc.title  Type: BYTE_ARRAY  Logical: STRING  Converted: UTF8     │
│ Values: 3  Nulls: 0  Pages: 1                                             │
│ Size: 85 B → 91 B (1.07x)  Min: "A"  Max: "Z"                             │
└───────────────────────────────────────────────────────────────────────────┘
┌─ Pages (↑↓ to navigate, Enter=view values) ───────────────────────────────┐
│  #  │ Page Type  │ Offset │ Comp  │ Uncomp │ Val │ Encoding │ Min │ Max  │
│  0  │ DATA_PAGE  │ 0x4B2A │ 85 B  │  91 B  │  3  │ PLAIN    │ "A" │ "Z"  │
└───────────────────────────────────────────────────────────────────────────┘
 Keys: ESC=back, s=schema, ↑↓=scroll, Enter=see item details
```

Features:
- **Column Chunk Info**: Detailed metadata including logical/converted types, page count (3-line header)
- **Statistics**: Min/Max values at both column chunk and page level, null counts
- **Page List**: All pages with offsets, sizes, encodings, and Min/Max statistics
- **Status Line**: Consistent keyboard shortcuts
- **Press Enter**: View decoded page content

### Page Content View

Press `Enter` on a page:

```
┌─ Page Info ───────────────────────────────────────────────────────────────┐
│ Page Type: DATA_PAGE  Offset: 0x4B2A  Size: 85 B → 91 B (1.07x)           │
│ Values: 3  Encoding: PLAIN                                                │
│ Min: "A"  Max: "Z"                                                        │
└───────────────────────────────────────────────────────────────────────────┘
┌─ Page Content (↑↓ to navigate) ───────────────────────────────────────────┐
│  #  │ Value                                                               │
│  1  │ "Alice's Adventures in Wonderland"                                  │
│  2  │ "The Great Gatsby"                                                  │
│  3  │ "To Kill a Mockingbird"                                             │
└───────────────────────────────────────────────────────────────────────────┘
 Keys: ESC=back, s=schema, ↑↓=scroll
```

Features:
- **Page Info**: Type, offset, sizes, total values count, encoding, Min/Max statistics (3-line header)
- **All Values**: Displays all decoded values from the page
- **Smart Formatting**: UTF-8 strings, hex for binary, NULL handling
- **Row Numbers**: Numbered for easy reference
- **Status Line**: Consistent keyboard shortcuts

### Schema Viewer

Press `s` from any view:

Shows schema in multiple formats:
- **JSON format**: Structured JSON representation with type information
- **Raw format**: Internal parquet-go schema structure
- **Go Struct**: Generated Go struct code ready to use
- **CSV format**: Column definitions in CSV format (name, type, repetition)

Each format supports:
- Pretty/compact toggle (JSON and Raw) with 'p' key
- Format cycling with 'v' key
- Copy to clipboard with 'c' key
- Full scrolling support

## HTTP API

The HTTP API provides programmatic access to all Parquet file metadata and content.

### Quick Examples

```bash
# Get file metadata
curl http://localhost:8080/info

# Get schema in different formats
curl http://localhost:8080/schema/go
curl http://localhost:8080/schema/json
curl http://localhost:8080/schema/raw
curl http://localhost:8080/schema/csv

# Get all row groups
curl http://localhost:8080/rowgroups

# Get column chunks for row group 0
curl http://localhost:8080/rowgroups/0/columnchunks

# Get pages for a column chunk
curl http://localhost:8080/rowgroups/0/columnchunks/0/pages

# Get page content (actual data values)
curl http://localhost:8080/rowgroups/0/columnchunks/0/pages/0/content
```

### Available Endpoints

- `GET /info` - File metadata
- `GET /schema/{format}` - Schema in Go, JSON, Raw, or CSV format
- `GET /rowgroups` - All row groups
- `GET /rowgroups/{rgIndex}` - Specific row group
- `GET /rowgroups/{rgIndex}/columnchunks` - All column chunks
- `GET /rowgroups/{rgIndex}/columnchunks/{colIndex}` - Specific column chunk
- `GET /rowgroups/{rgIndex}/columnchunks/{colIndex}/pages` - All pages
- `GET /rowgroups/{rgIndex}/columnchunks/{colIndex}/pages/{pageIndex}` - Page info
- `GET /rowgroups/{rgIndex}/columnchunks/{colIndex}/pages/{pageIndex}/content` - Page content

### OpenAPI/Swagger Documentation

The API is documented using OpenAPI 3.0 specification in [swagger.yaml](swagger.yaml). You can:
- Import it into API testing tools like Postman or Insomnia
- Generate client libraries using OpenAPI generators
- View it in Swagger UI or similar tools

## Architecture

The tool follows a clean three-layer architecture:

```
┌──────────────────────┐
│   TUI (cmd/)         │  ← Terminal UI or external HTTP clients
│  Uses HTTP Client    │
└──────────┬───────────┘
           │ HTTP
           ↓
┌──────────────────────┐
│  HTTP Service        │  ← RESTful API endpoints
│  (service/)          │     (embedded or standalone)
└──────────┬───────────┘
           │
           ↓
┌──────────────────────┐
│  Model Layer         │  ← Pure business logic
│  (model/)            │     (no UI or HTTP dependencies)
└──────────────────────┘
```

**Benefits:**
- Model layer is reusable and testable in isolation
- HTTP API can be consumed by any client (TUI, web UI, CLI tools, etc.)
- TUI and server modes share the same codebase
- Clean separation of concerns

## Dependencies

### Core Dependencies
- [**tview**](https://github.com/rivo/tview) - TUI framework for building terminal interfaces
- [**tcell**](https://github.com/gdamore/tcell) - Low-level terminal handling and keyboard/mouse events
- [**parquet-go**](https://github.com/hangxie/parquet-go) - Pure Go Parquet file reading and processing
- [**parquet-tools**](https://github.com/hangxie/parquet-tools) - Parquet schema utilities and file I/O
- [**thrift**](https://github.com/apache/thrift) - Apache Thrift for reading Parquet page headers
- [**clipboard**](https://github.com/atotto/clipboard) - Cross-platform clipboard support

### HTTP & API
- [**gorilla/mux**](https://github.com/gorilla/mux) - HTTP router for RESTful API endpoints
- [**htmx**](https://htmx.org/) - Modern web interactions without JavaScript (embedded in web UI templates)

### CLI & Utilities
- [**kong**](https://github.com/alecthomas/kong) - Command-line parser
- [**kongplete**](https://github.com/willabides/kongplete) - Shell completion for kong
- [**complete**](https://github.com/posener/complete) - Bash completion

### Testing
- [**testify**](https://github.com/stretchr/testify) - Testing toolkit with assertions

### Cloud Storage Support (via parquet-tools)
The tool inherits support for cloud storage backends:
- **AWS S3** (via aws-sdk-go-v2)
- **Google Cloud Storage** (via cloud.google.com/go/storage)
- **Azure Blob Storage** (via azure-sdk-for-go)
- **HDFS** and **HTTP/HTTPS**

## License

See [LICENSE](LICENSE) file for details.
