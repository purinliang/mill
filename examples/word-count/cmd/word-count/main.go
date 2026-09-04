package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"

	"github.com/purinliang/mill/examples/word-count"
	"github.com/purinliang/mill/internal/workload"
)

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:]); err != nil {
		log.Fatalf("run word-count workload: %v", err)
	}
}

func run(arguments []string) error {
	invocation, err := workload.ParseArgs(arguments)
	if err != nil {
		return err
	}
	if len(invocation.ExecutableArgs) != 0 {
		return errors.New("the word-count workload accepts no executable arguments")
	}

	inputFilename, err := localFilename("input", invocation.InputURI)
	if err != nil {
		return err
	}
	outputFilename, err := localFilename("output", invocation.OutputURI)
	if err != nil {
		return err
	}
	if inputFilename == outputFilename {
		return errors.New("input and output must be different files")
	}

	input, err := os.Open(inputFilename)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer input.Close()

	info, err := input.Stat()
	if err != nil {
		return fmt.Errorf("inspect input: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("input must be a regular file")
	}
	if invocation.InputEndByte > info.Size() {
		return fmt.Errorf("input range ends at byte %d beyond file size %d", invocation.InputEndByte, info.Size())
	}

	length := invocation.InputEndByte - invocation.InputStartByte
	counts, err := wordcount.CountRecords(io.NewSectionReader(input, invocation.InputStartByte, length))
	if err != nil {
		return err
	}
	if err := publishCounts(outputFilename, counts); err != nil {
		return err
	}
	return nil
}

func publishCounts(outputFilename string, counts map[string]uint64) error {
	if err := os.MkdirAll(filepath.Dir(outputFilename), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(outputFilename), ".mill-word-count-*.tmp")
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

	if err := wordcount.Write(temporary, counts); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary output: %w", err)
	}
	if err := os.Rename(temporaryFilename, outputFilename); err != nil {
		return fmt.Errorf("publish output: %w", err)
	}
	committed = true
	return nil
}

func localFilename(name, rawURI string) (string, error) {
	parsed, err := url.ParseRequestURI(rawURI)
	if err != nil || parsed.Scheme != "file" || parsed.Host != "" || parsed.User != nil || parsed.Opaque != "" {
		return "", fmt.Errorf("%s URI must be an absolute local file:// URI", name)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("%s URI must not contain a query or fragment", name)
	}
	filename, err := url.PathUnescape(parsed.EscapedPath())
	if err != nil || !filepath.IsAbs(filename) {
		return "", fmt.Errorf("%s URI must contain an absolute path", name)
	}
	return filepath.Clean(filename), nil
}
