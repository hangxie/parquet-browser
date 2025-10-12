package cmd

import (
	"encoding/json"
	"fmt"
	"go/format"
	"strings"

	"github.com/atotto/clipboard"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	pschema "github.com/hangxie/parquet-tools/schema"
)

// schemaViewer encapsulates schema viewing functionality
type schemaViewer struct {
	app           *BrowseApp
	schemaFormats []string
	currentFormat int
	isPretty      bool
	textView      *tview.TextView
	titleBar      *tview.TextView
	statusBar     *tview.TextView
}

func (sv *schemaViewer) show() {
	sv.textView.SetBorder(true)
	sv.updateDisplay()

	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(sv.titleBar, 1, 0, false).
		AddItem(sv.textView, 0, 1, true).
		AddItem(sv.statusBar, 1, 0, false)

	flex.SetBorder(true)
	flex.SetInputCapture(sv.handleInput)

	sv.app.pages.AddPage("schema", flex, true, true)
}

func (sv *schemaViewer) handleInput(event *tcell.EventKey) *tcell.EventKey {
	if event.Key() == tcell.KeyEscape {
		sv.app.pages.RemovePage("schema")
		return nil
	}
	if event.Key() == tcell.KeyRune {
		switch event.Rune() {
		case 'v', 'V':
			sv.toggleFormat()
			return nil
		case 'p', 'P':
			sv.togglePretty()
			return nil
		case 'c', 'C':
			sv.copyToClipboard()
			return nil
		}
	}
	return event
}

func (sv *schemaViewer) toggleFormat() {
	sv.currentFormat = (sv.currentFormat + 1) % len(sv.schemaFormats)
	sv.statusBar.SetText("")
	sv.updateDisplay()
}

func (sv *schemaViewer) togglePretty() {
	format := sv.schemaFormats[sv.currentFormat]
	if format == "json" || format == "raw" {
		sv.isPretty = !sv.isPretty
		sv.statusBar.SetText("")
		sv.updateDisplay()
	}
}

func (sv *schemaViewer) copyToClipboard() {
	text := sv.textView.GetText(false)
	err := clipboard.WriteAll(text)
	if err != nil {
		sv.statusBar.SetText(fmt.Sprintf("[red]Failed to copy: %v[-]", err))
	} else {
		sv.statusBar.SetText(fmt.Sprintf("[green]Copied %s schema to clipboard![-]", strings.ToUpper(sv.schemaFormats[sv.currentFormat])))
	}
}

func (sv *schemaViewer) updateDisplay() {
	sv.updateTitle()

	schemaRoot, err := pschema.NewSchemaTree(sv.app.parquetReader, pschema.SchemaOption{FailOnInt96: false})
	if err != nil {
		sv.textView.SetText(fmt.Sprintf("Error creating schema tree: %v", err))
		return
	}

	schemaText := sv.formatSchema(schemaRoot)
	sv.textView.SetText(schemaText)
}

func (sv *schemaViewer) updateTitle() {
	format := sv.schemaFormats[sv.currentFormat]
	var titleText string

	if format == "json" || format == "raw" {
		mode := "Pretty"
		if !sv.isPretty {
			mode = "Compact"
		}
		titleText = fmt.Sprintf("[yellow]Schema [%s - %s] | ESC=close, v=toggle format, p=toggle pretty/compact, c=copy[-]", strings.ToUpper(format), mode)
	} else if format == "go" {
		titleText = "[yellow]Schema [Go Struct] | ESC=close, v=toggle format, c=copy[-]"
	} else if format == "csv" {
		titleText = "[yellow]Schema [CSV] | ESC=close, v=toggle format, c=copy[-]"
	} else {
		titleText = fmt.Sprintf("[yellow]Schema [%s] | ESC=close, v=toggle format, c=copy[-]", strings.ToUpper(format))
	}

	sv.titleBar.SetText(titleText)
}

func (sv *schemaViewer) formatSchema(schemaRoot *pschema.SchemaNode) string {
	format := sv.schemaFormats[sv.currentFormat]

	switch format {
	case "json":
		return sv.formatJSONSchema(schemaRoot)
	case "raw":
		return sv.formatRawSchema(schemaRoot)
	case "go":
		return sv.formatGoSchema(schemaRoot)
	case "csv":
		return sv.formatCSVSchema(schemaRoot)
	default:
		return ""
	}
}

func (sv *schemaViewer) formatJSONSchema(schemaRoot *pschema.SchemaNode) string {
	jsonSchema := schemaRoot.JSONSchema()
	var jsonObj interface{}

	if err := json.Unmarshal([]byte(jsonSchema), &jsonObj); err != nil {
		return jsonSchema
	}

	if sv.isPretty {
		prettyBytes, _ := json.MarshalIndent(jsonObj, "", "  ")
		return string(prettyBytes)
	}

	compactBytes, _ := json.Marshal(jsonObj)
	return string(compactBytes)
}

func (sv *schemaViewer) formatRawSchema(schemaRoot *pschema.SchemaNode) string {
	if sv.isPretty {
		rawSchema, _ := json.MarshalIndent(*schemaRoot, "", "  ")
		return string(rawSchema)
	}

	rawSchema, _ := json.Marshal(*schemaRoot)
	return string(rawSchema)
}

func (sv *schemaViewer) formatGoSchema(schemaRoot *pschema.SchemaNode) string {
	goStruct, err := schemaRoot.GoStruct(false)
	if err != nil {
		return fmt.Sprintf("Error generating Go struct: %v", err)
	}

	// Format the Go code using go/format
	formatted, err := format.Source([]byte(goStruct))
	if err != nil {
		// If formatting fails, return the original unformatted code
		return goStruct
	}

	return string(formatted)
}

func (sv *schemaViewer) formatCSVSchema(schemaRoot *pschema.SchemaNode) string {
	csvSchema, err := schemaRoot.CSVSchema()
	if err != nil {
		return fmt.Sprintf("Error generating CSV schema: %v", err)
	}
	return csvSchema
}
