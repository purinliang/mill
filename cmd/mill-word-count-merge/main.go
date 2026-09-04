package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/purinliang/mill/examples/word-count"
)

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:], os.Stdout); err != nil {
		log.Fatalf("merge word counts: %v", err)
	}
}

func run(filenames []string, output io.Writer) error {
	if len(filenames) == 0 {
		return errors.New("at least one partial count filename is required")
	}

	counts := make(map[string]uint64)
	for _, filename := range filenames {
		input, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("open partial counts %q: %w", filename, err)
		}
		mergeErr := wordcount.Merge(input, counts)
		closeErr := input.Close()
		if mergeErr != nil {
			return fmt.Errorf("merge partial counts %q: %w", filename, mergeErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close partial counts %q: %w", filename, closeErr)
		}
	}
	return wordcount.Write(output, counts)
}
