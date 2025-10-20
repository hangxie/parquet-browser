package cmd

import (
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

	// Fetch schema from HTTP API
	var schemaText string
	var err error

	format := sv.schemaFormats[sv.currentFormat]
	switch format {
	case "go":
		schemaText, err = sv.app.httpClient.GetSchemaGo()
	case "json":
		schemaText, err = sv.app.httpClient.GetSchemaJSON(sv.isPretty)
	case "csv":
		schemaText, err = sv.app.httpClient.GetSchemaCSV()
	case "raw":
		schemaText, err = sv.app.httpClient.GetSchemaRaw(sv.isPretty)
	default:
		schemaText, err = sv.app.httpClient.GetSchemaGo()
	}

	if err != nil {
		sv.textView.SetText(fmt.Sprintf("Error fetching schema: %v", err))
		return
	}

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
