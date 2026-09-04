package wordcount

import (
	"strings"
	"testing"
)

func TestCountRecordsNormalizesEnglishTokens(t *testing.T) {
	input := strings.NewReader(
		`{"text":"Hello, HELLO. well-known end-to-end -trim- double--dash version2 42; don't."}` + "\n",
	)

	got, err := CountRecords(input)
	if err != nil {
		t.Fatalf("count records: %v", err)
	}
	want := map[string]uint64{
		"42":         1,
		"dash":       1,
		"don":        1,
		"double":     1,
		"end-to-end": 1,
		"hello":      2,
		"t":          1,
		"trim":       1,
		"version2":   1,
		"well-known": 1,
	}
	if !equalCounts(got, want) {
		t.Fatalf("counts = %#v, want %#v", got, want)
	}
}

func TestCountRecordsRequiresText(t *testing.T) {
	for _, input := range []string{"not-json\n", "{}\n"} {
		if _, err := CountRecords(strings.NewReader(input)); err == nil {
			t.Errorf("CountRecords(%q) succeeded, want an error", input)
		}
	}
}

func TestWriteSortsAndMergeSumsCounts(t *testing.T) {
	var first strings.Builder
	if err := Write(&first, map[string]uint64{"well-known": 1, "a": 2}); err != nil {
		t.Fatalf("write first counts: %v", err)
	}
	if got, want := first.String(), "{\"word\":\"a\",\"count\":2}\n{\"word\":\"well-known\",\"count\":1}\n"; got != want {
		t.Fatalf("encoded counts = %q, want %q", got, want)
	}

	merged := make(map[string]uint64)
	if err := Merge(strings.NewReader(first.String()), merged); err != nil {
		t.Fatalf("merge first counts: %v", err)
	}
	if err := Merge(strings.NewReader("{\"word\":\"a\",\"count\":3}\n"), merged); err != nil {
		t.Fatalf("merge second counts: %v", err)
	}
	want := map[string]uint64{"a": 5, "well-known": 1}
	if !equalCounts(merged, want) {
		t.Fatalf("merged counts = %#v, want %#v", merged, want)
	}
}

func TestMergeRejectsInvalidCounts(t *testing.T) {
	for _, input := range []string{
		"{\"word\":\"Not-Normalized\",\"count\":1}\n",
		"{\"word\":\"valid\",\"count\":0}\n",
		"not-json\n",
	} {
		if err := Merge(strings.NewReader(input), make(map[string]uint64)); err == nil {
			t.Errorf("Merge(%q) succeeded, want an error", input)
		}
	}
	if err := Merge(strings.NewReader(""), nil); err == nil {
		t.Fatal("Merge with nil destination succeeded, want an error")
	}
}

func equalCounts(left, right map[string]uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for word, count := range left {
		if right[word] != count {
			return false
		}
	}
	return true
}
