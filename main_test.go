package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewParserRegistersCommands(t *testing.T) {
	parser := newParser()

	var commandNames []string
	for _, child := range parser.Model.Children {
		commandNames = append(commandNames, child.Name)
	}

	require.ElementsMatch(t, []string{"tui", "serve", "web-ui", "version"}, commandNames)
}

func TestNewParserParsesVersionCommand(t *testing.T) {
	parser := newParser()

	ctx, err := parser.Parse([]string{"version"})
	require.NoError(t, err)
	require.Equal(t, "version", ctx.Command())
}
