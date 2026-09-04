package wordcount

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

const maxJSONLRecordBytes = 16 << 20

var (
	tokenPattern      = regexp.MustCompile(`[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*`)
	normalizedPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
)

type Entry struct {
	Word  string `json:"word"`
	Count uint64 `json:"count"`
}

type textRecord struct {
	Text *string `json:"text"`
}

func CountRecords(input io.Reader) (map[string]uint64, error) {
	counts := make(map[string]uint64)
	scanner := newJSONLScanner(input)
	for recordNumber := 1; scanner.Scan(); recordNumber++ {
		var record textRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode input record %d: %w", recordNumber, err)
		}
		if record.Text == nil {
			return nil, fmt.Errorf("decode input record %d: text is required", recordNumber)
		}
		for _, token := range tokenPattern.FindAllString(*record.Text, -1) {
			counts[strings.ToLower(token)]++
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read JSONL input: %w", err)
	}
	return counts, nil
}

func Merge(input io.Reader, counts map[string]uint64) error {
	if counts == nil {
		return errors.New("destination counts map is required")
	}

	scanner := newJSONLScanner(input)
	for recordNumber := 1; scanner.Scan(); recordNumber++ {
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return fmt.Errorf("decode count record %d: %w", recordNumber, err)
		}
		if !normalizedPattern.MatchString(entry.Word) {
			return fmt.Errorf("decode count record %d: word must be a normalized token", recordNumber)
		}
		if entry.Count == 0 {
			return fmt.Errorf("decode count record %d: count must be positive", recordNumber)
		}
		if ^uint64(0)-counts[entry.Word] < entry.Count {
			return fmt.Errorf("merge count record %d: count for %q overflows uint64", recordNumber, entry.Word)
		}
		counts[entry.Word] += entry.Count
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read count JSONL: %w", err)
	}
	return nil
}

func Write(output io.Writer, counts map[string]uint64) error {
	words := make([]string, 0, len(counts))
	for word, count := range counts {
		if !normalizedPattern.MatchString(word) {
			return fmt.Errorf("write counts: %q is not a normalized token", word)
		}
		if count == 0 {
			return fmt.Errorf("write counts: count for %q must be positive", word)
		}
		words = append(words, word)
	}
	sort.Strings(words)

	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	for _, word := range words {
		if err := encoder.Encode(Entry{Word: word, Count: counts[word]}); err != nil {
			return fmt.Errorf("encode count for %q: %w", word, err)
		}
	}
	return nil
}

func newJSONLScanner(input io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), maxJSONLRecordBytes+1)
	return scanner
}
