package objectstore_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/objectstore"
)

func TestOpenReadsCompleteFileAndS3Objects(t *testing.T) {
	const contents = "first\nsecond\n"

	t.Run("file", func(t *testing.T) {
		filename := filepath.Join(t.TempDir(), "input.jsonl")
		if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		store, err := objectstore.New(context.Background(), objectstore.Config{})
		if err != nil {
			t.Fatal(err)
		}
		assertObjectContents(t, store, fileURI(filename), contents)
	})

	t.Run("s3", func(t *testing.T) {
		t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
		t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodGet || request.URL.Path != "/mill-input/records.jsonl" {
				http.Error(response, "unexpected request", http.StatusNotFound)
				return
			}
			if request.Header.Get("Range") != "" {
				t.Errorf("unexpected Range header %q", request.Header.Get("Range"))
			}
			response.Header().Set("Content-Length", "13")
			_, _ = io.WriteString(response, contents)
		}))
		defer server.Close()

		store, err := objectstore.New(context.Background(), objectstore.Config{
			Region:   "us-east-1",
			Endpoint: server.URL,
		})
		if err != nil {
			t.Fatal(err)
		}
		assertObjectContents(t, store, "s3://mill-input/records.jsonl", contents)
	})
}

func TestFilePutDoesNotReplaceExistingOutputAfterReadFailure(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "result.jsonl")
	if err := os.WriteFile(output, []byte("stable\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := objectstore.New(context.Background(), objectstore.Config{})
	if err != nil {
		t.Fatal(err)
	}

	err = store.Put(context.Background(), fileURI(output), failingReadSeeker{})
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

func TestFileReadsRejectDirectoriesAndOutOfBoundsRanges(t *testing.T) {
	store, err := objectstore.New(context.Background(), objectstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if _, err := store.Open(context.Background(), fileURI(directory)); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("Open directory error = %v", err)
	}
	if _, err := store.OpenRange(context.Background(), fileURI(directory), 0, 1); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("OpenRange directory error = %v", err)
	}

	filename := filepath.Join(directory, "short.txt")
	if err := os.WriteFile(filename, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenRange(context.Background(), fileURI(filename), 0, 6); err == nil || !strings.Contains(err.Error(), "beyond object size") {
		t.Fatalf("out-of-bounds OpenRange error = %v", err)
	}
}

type failingReadSeeker struct{}

func (failingReadSeeker) Read([]byte) (int, error) {
	return 0, errors.New("injected read failure")
}

func (failingReadSeeker) Seek(int64, int) (int64, error) {
	return 0, nil
}

func assertObjectContents(t *testing.T, store *objectstore.Store, uri, expected string) {
	t.Helper()
	reader, err := store.Open(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := io.ReadAll(reader)
	if err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if string(contents) != expected {
		t.Fatalf("object contents = %q, want %q", contents, expected)
	}
}

func fileURI(filename string) string {
	return (&url.URL{Scheme: "file", Path: filename}).String()
}
