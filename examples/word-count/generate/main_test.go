package main

import (
	"strings"
	"testing"
)

func TestReadParagraphsJoinsWrappedLinesAndSkipsHeading(t *testing.T) {
	input := strings.NewReader("Economy\n\nfirst wrapped\nparagraph\n\nsecond paragraph\n")
	paragraphs, err := readParagraphs(input)
	if err != nil {
		t.Fatalf("read paragraphs: %v", err)
	}
	want := []string{"first wrapped paragraph", "second paragraph"}
	if len(paragraphs) != len(want) {
		t.Fatalf("paragraphs = %#v, want %#v", paragraphs, want)
	}
	for index := range want {
		if paragraphs[index] != want[index] {
			t.Fatalf("paragraph %d = %q, want %q", index, paragraphs[index], want[index])
		}
	}
}

func TestGroupParagraphsIsDeterministicAndKeepsOrder(t *testing.T) {
	paragraphs := make([]string, 60)
	for index := range paragraphs {
		paragraphs[index] = string(rune('A' + index))
	}
	config := groupingConfig{Seed: 205, NewRecordPercent: 20}
	first := groupParagraphs(paragraphs, config)
	second := groupParagraphs(paragraphs, config)
	wantGroupSizes := []int{2, 9, 3, 2, 5, 15, 10, 1, 4, 5, 3, 1}
	if len(first) != len(wantGroupSizes) {
		t.Fatalf("record count = %d, want %d", len(first), len(wantGroupSizes))
	}
	if len(first) != len(second) {
		t.Fatalf("record counts differ: %d and %d", len(first), len(second))
	}
	for index := range first {
		if first[index] != second[index] {
			t.Fatalf("record %d differs: %#v and %#v", index, first[index], second[index])
		}
		if size := len(strings.Split(first[index].Text, "\n\n")); size != wantGroupSizes[index] {
			t.Fatalf("record %d contains %d paragraphs, want %d", index, size, wantGroupSizes[index])
		}
	}
	joined := make([]string, 0, len(paragraphs))
	for _, record := range first {
		joined = append(joined, strings.Split(record.Text, "\n\n")...)
	}
	if strings.Join(joined, "|") != strings.Join(paragraphs, "|") {
		t.Fatal("grouping changed paragraph order or contents")
	}
}
