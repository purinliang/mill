package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/purinliang/mill/examples/word-count"
	"github.com/purinliang/mill/internal/objectstore"
	"github.com/purinliang/mill/internal/workload"
)

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:]); err != nil {
		log.Fatalf("run word-count workload: %v", err)
	}
}

func run(arguments []string) error {
	ctx := context.Background()
	objects, err := objectstore.New(ctx, objectstore.Config{
		Region: os.Getenv("AWS_REGION"), Endpoint: os.Getenv("MILL_S3_ENDPOINT"),
	})
	if err != nil {
		return err
	}
	return runWithStore(ctx, arguments, objects)
}

type taskObjects interface {
	OpenRange(context.Context, string, int64, int64) (io.ReadCloser, error)
	Put(context.Context, string, io.ReadSeeker) error
}

func runWithStore(ctx context.Context, arguments []string, objects taskObjects) error {
	invocation, err := workload.ParseArgs(arguments)
	if err != nil {
		return err
	}
	if len(invocation.ExecutableArgs) != 0 {
		return errors.New("the word-count workload accepts no executable arguments")
	}

	if invocation.InputURI == invocation.OutputURI {
		return errors.New("input and output must be different objects")
	}

	input, err := objects.OpenRange(ctx, invocation.InputURI, invocation.InputStartByte, invocation.InputEndByte)
	if err != nil {
		return fmt.Errorf("open input range: %w", err)
	}
	defer input.Close()

	counts, err := wordcount.CountRecords(input)
	if err != nil {
		return err
	}
	var output bytes.Buffer
	if err := wordcount.Write(&output, counts); err != nil {
		return err
	}
	if err := objects.Put(ctx, invocation.OutputURI, bytes.NewReader(output.Bytes())); err != nil {
		return fmt.Errorf("publish output: %w", err)
	}
	return nil
}
