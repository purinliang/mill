// This file tests local object reads, ranges, and atomic publication.

package objectstore_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileOpenRangeAndPut(t *testing.T) {
	store := newFileStore(t)
	directory := t.TempDir()
	input := filepath.Join(directory, "input.jsonl")
	contents := "first\nsecond\nthird\n"
	if err := os.WriteFile(input, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	assertObjectContents(t, store, fileURI(input), contents)

	reader, err := store.OpenRange(
		context.Background(), fileURI(input), 6, 13,
	)
	if err != nil {
		t.Fatal(err)
	}
	rangeContents, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if string(rangeContents) != "second\n" {
		t.Fatalf("range = %q, want second line", rangeContents)
	}

	output := filepath.Join(directory, "nested", "result.jsonl")
	if err := store.Put(
		context.Background(),
		fileURI(output),
		strings.NewReader("result\n"),
	); err != nil {
		t.Fatal(err)
	}
	assertObjectContents(t, store, fileURI(output), "result\n")
}

func TestFilePutDoesNotReplaceOutputAfterReadFailure(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "result.jsonl")
	if err := os.WriteFile(output, []byte("stable\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newFileStore(t)

	err := store.Put(
		context.Background(),
		fileURI(output),
		failingReadSeeker{},
	)
	if err == nil || !strings.Contains(err.Error(), "write output") {
		t.Fatalf("Put error = %v", err)
	}
	published, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(published) != "stable\n" {
		t.Fatalf("published output = %q", published)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "result.jsonl" {
		t.Fatalf("directory entries after failed Put = %v", entries)
	}
}

func TestFileReadsRejectInvalidTargetsAndRanges(t *testing.T) {
	store := newFileStore(t)
	directory := t.TempDir()
	if _, err := store.Open(
		context.Background(),
		fileURI(directory),
	); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("Open directory error = %v", err)
	}
	if _, err := store.OpenRange(
		context.Background(),
		fileURI(directory),
		0,
		1,
	); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("OpenRange directory error = %v", err)
	}

	filename := filepath.Join(directory, "short.txt")
	if err := os.WriteFile(filename, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenRange(
		context.Background(),
		fileURI(filename),
		0,
		6,
	); err == nil || !strings.Contains(err.Error(), "beyond object size") {
		t.Fatalf("out-of-bounds OpenRange error = %v", err)
	}
}

func TestFileReadsReportMissingObjects(t *testing.T) {
	store := newFileStore(t)
	missing := fileURI(filepath.Join(t.TempDir(), "missing.jsonl"))
	if _, err := store.Open(context.Background(), missing); err == nil {
		t.Fatal("Open succeeded for a missing file")
	}
	if _, err := store.OpenRange(
		context.Background(), missing, 0, 1,
	); err == nil {
		t.Fatal("OpenRange succeeded for a missing file")
	}
}

func TestFilePutReportsPublicationPathFailures(t *testing.T) {
	store := newFileStore(t)

	t.Run("parent is a regular file", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "parent")
		if err := os.WriteFile(
			parent,
			[]byte("not a directory"),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
		err := store.Put(
			context.Background(),
			fileURI(filepath.Join(parent, "result.jsonl")),
			strings.NewReader("result"),
		)
		if err == nil || !strings.Contains(
			err.Error(),
			"create output directory",
		) {
			t.Fatalf("Put error = %v", err)
		}
	})

	t.Run("destination is a directory", func(t *testing.T) {
		destination := filepath.Join(t.TempDir(), "result.jsonl")
		if err := os.Mkdir(destination, 0o700); err != nil {
			t.Fatal(err)
		}
		err := store.Put(
			context.Background(),
			fileURI(destination),
			strings.NewReader("result"),
		)
		if err == nil || !strings.Contains(err.Error(), "publish output") {
			t.Fatalf("Put error = %v", err)
		}
	})
}

type failingReadSeeker struct{}

func (failingReadSeeker) Read([]byte) (int, error) {
	return 0, errors.New("injected read failure")
}

func (failingReadSeeker) Seek(int64, int) (int64, error) {
	return 0, nil
}
