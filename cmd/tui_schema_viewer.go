package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/atotto/clipboard"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// schemaViewer encapsulates schema viewing functionality
type schemaViewer struct {
	app           *TUIApp
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
		case 'g', 'G':
			sv.switchToFormat("go")
			return nil
		case 'j', 'J':
			sv.switchToFormat("json")
			return nil
		case 'r', 'R':
			sv.switchToFormat("raw")
			return nil
		case 'c', 'C':
			sv.switchToFormat("csv")
			return nil
		case 'p', 'P':
			sv.togglePretty()
			return nil
		case 'y', 'Y':
			sv.copyToClipboard()
			return nil
		}
	}
	return event
}

func (sv *schemaViewer) switchToFormat(format string) {
	// Find the index of the requested format
	for i, f := range sv.schemaFormats {
		if f == format {
			sv.currentFormat = i
			sv.statusBar.SetText("")
			sv.updateDisplay()
			return
		}
	}
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

	// Fetch schema from HTTP API
	var schemaText string
	var err error

	format := sv.schemaFormats[sv.currentFormat]
	switch format {
	case "go":
		schemaText, err = sv.app.httpClient.GetSchemaGo()
	case "csv":
		schemaText, err = sv.app.httpClient.GetSchemaCSV()
	case "json":
		schemaText, err = sv.app.httpClient.GetSchemaJSON()
		if err == nil && sv.isPretty {
			schemaText = sv.formatJSON(schemaText)
		}
	case "raw":
		schemaText, err = sv.app.httpClient.GetSchemaRaw()
		if err == nil && sv.isPretty {
			schemaText = sv.formatJSON(schemaText)
		}
	default:
		schemaText, err = sv.app.httpClient.GetSchemaGo()
	}

	if err != nil {
		sv.textView.SetText(fmt.Sprintf("Error fetching schema: %v", err))
		return
	}

	sv.textView.SetText(schemaText)
}

// formatJSON formats JSON string with indentation
func (sv *schemaViewer) formatJSON(jsonStr string) string {
	var jsonObj interface{}
	if err := json.Unmarshal([]byte(jsonStr), &jsonObj); err != nil {
		return jsonStr // Return original if unmarshal fails
	}

	prettyBytes, err := json.MarshalIndent(jsonObj, "", "  ")
	if err != nil {
		return jsonStr // Return original if marshal fails
	}

	return string(prettyBytes)
}

func (sv *schemaViewer) updateTitle() {
	format := sv.schemaFormats[sv.currentFormat]
	var titleText string

	if format == "json" || format == "raw" {
		mode := "Pretty"
		if !sv.isPretty {
			mode = "Compact"
		}
		titleText = fmt.Sprintf("[yellow]Schema [%s - %s] | ESC=close, g=go, j=json, r=raw, c=csv, p=pretty/compact, y=copy[-]", strings.ToUpper(format), mode)
	} else if format == "go" {
		titleText = "[yellow]Schema [Go Struct] | ESC=close, g=go, j=json, r=raw, c=csv, y=copy[-]"
	} else if format == "csv" {
		titleText = "[yellow]Schema [CSV] | ESC=close, g=go, j=json, r=raw, c=csv, y=copy[-]"
	} else {
		titleText = fmt.Sprintf("[yellow]Schema [%s] | ESC=close, g=go, j=json, r=raw, c=csv, y=copy[-]", strings.ToUpper(format))
	}

	sv.titleBar.SetText(titleText)
}
