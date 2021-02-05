package main

import (
	"bufio"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJournaldScanner_ScrubbingLinesWithPrefix(t *testing.T) {
	log := `{"MESSAGE":"[24-1] Hello"}
{"MESSAGE":"[25-1] World"}
{"MESSAGE":"[26-1] Yolo"}
{"MESSAGE":"[1-12] Brolo"}
`

	scanner := bufio.NewScanner(strings.NewReader(log))
	journaldScanner := &JournaldScanner{
		scanner:     scanner,
		nextMessage: new(JournalMessage),
	}

	assert.True(t, journaldScanner.Scan())
	assert.Equal(t, journaldScanner.Text(), "Hello")
	assert.True(t, journaldScanner.Scan())
	assert.Equal(t, journaldScanner.Text(), "World")
	assert.True(t, journaldScanner.Scan())
	assert.Equal(t, journaldScanner.Text(), "Yolo")
	assert.True(t, journaldScanner.Scan())
	assert.Equal(t, journaldScanner.Text(), "Brolo")
	assert.False(t, journaldScanner.Scan())
}
