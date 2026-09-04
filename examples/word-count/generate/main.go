package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const groupingAlgorithm = "splitmix64-boundary-v1"

type groupingConfig struct {
	Algorithm        string `json:"algorithm"`
	Seed             uint64 `json:"seed"`
	NewRecordPercent int    `json:"new_record_percent"`
	ParagraphLimit   int    `json:"paragraph_limit"`
}

type textRecord struct {
	Text string `json:"text"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "generate word-count input: %v\n", err)
		os.Exit(1)
	}
}

func run(arguments []string, output io.Writer) error {
	flags := flag.NewFlagSet("generate-word-count-input", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	sourceFilename := flags.String("source", "examples/word-count/walden-economy.txt", "plain-text source")
	configFilename := flags.String("config", "examples/word-count/record-config.json", "deterministic record-grouping configuration")
	outputFilename := flags.String("output", "/tmp/mill-word-count/walden-economy.jsonl", "generated JSONL output")
	if err := flags.Parse(arguments); err != nil {
		return fmt.Errorf("parse arguments: %w", err)
	}
	if len(flags.Args()) != 0 {
		return errors.New("unexpected positional arguments")
	}

	config, err := readConfig(*configFilename)
	if err != nil {
		return err
	}
	source, err := os.Open(*sourceFilename)
	if err != nil {
		return fmt.Errorf("open source text: %w", err)
	}
	paragraphs, parseErr := readParagraphs(source)
	closeErr := source.Close()
	if parseErr != nil {
		return parseErr
	}
	if closeErr != nil {
		return fmt.Errorf("close source text: %w", closeErr)
	}
	if len(paragraphs) < config.ParagraphLimit {
		return fmt.Errorf("source contains %d paragraphs, need %d", len(paragraphs), config.ParagraphLimit)
	}
	paragraphs = paragraphs[:config.ParagraphLimit]
	records := groupParagraphs(paragraphs, config)
	if err := writeRecords(*outputFilename, records); err != nil {
		return err
	}
	_, err = fmt.Fprintf(
		output,
		"Generated %d deterministic JSONL records from %d paragraphs at %s\n",
		len(records),
		len(paragraphs),
		*outputFilename,
	)
	return err
}

func readConfig(filename string) (groupingConfig, error) {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return groupingConfig{}, fmt.Errorf("read record-grouping config: %w", err)
	}
	var config groupingConfig
	decoder := json.NewDecoder(strings.NewReader(string(contents)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return groupingConfig{}, fmt.Errorf("decode record-grouping config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return groupingConfig{}, errors.New("record-grouping config must contain exactly one JSON object")
		}
		return groupingConfig{}, fmt.Errorf("decode trailing record-grouping config content: %w", err)
	}
	if config.Algorithm != groupingAlgorithm {
		return groupingConfig{}, fmt.Errorf("record-grouping algorithm must be %q", groupingAlgorithm)
	}
	if config.NewRecordPercent < 1 || config.NewRecordPercent > 99 {
		return groupingConfig{}, errors.New("new_record_percent must be between 1 and 99")
	}
	if config.ParagraphLimit < 1 {
		return groupingConfig{}, errors.New("paragraph_limit must be positive")
	}
	return config, nil
}

func readParagraphs(input io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	var paragraphs []string
	var lines []string
	flush := func() {
		if len(lines) == 0 {
			return
		}
		paragraphs = append(paragraphs, strings.Join(lines, " "))
		lines = nil
	}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			flush()
			continue
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read source text: %w", err)
	}
	flush()
	if len(paragraphs) == 0 || paragraphs[0] != "Economy" {
		return nil, errors.New("source text must begin with the Economy heading")
	}
	return paragraphs[1:], nil
}

func groupParagraphs(paragraphs []string, config groupingConfig) []textRecord {
	records := make([]textRecord, 0, len(paragraphs))
	current := make([]string, 0, 1)
	for index, paragraph := range paragraphs {
		if len(current) > 0 && shouldStartRecord(config.Seed, uint64(index), config.NewRecordPercent) {
			records = append(records, textRecord{Text: strings.Join(current, "\n\n")})
			current = current[:0]
		}
		current = append(current, paragraph)
	}
	if len(current) > 0 {
		records = append(records, textRecord{Text: strings.Join(current, "\n\n")})
	}
	return records
}

func shouldStartRecord(seed, boundary uint64, newRecordPercent int) bool {
	value := seed + boundary*0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31
	return value%100 < uint64(newRecordPercent)
}

func writeRecords(filename string, records []textRecord) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(filename), ".walden-word-count-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary output: %w", err)
	}
	temporaryFilename := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryFilename)
		}
	}()

	encoder := json.NewEncoder(temporary)
	encoder.SetEscapeHTML(false)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("encode JSONL record: %w", err)
		}
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close generated input: %w", err)
	}
	if err := os.Rename(temporaryFilename, filename); err != nil {
		return fmt.Errorf("publish generated input: %w", err)
	}
	committed = true
	return nil
}
