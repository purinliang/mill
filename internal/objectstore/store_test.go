// This file tests object URI parsing and local storage implementation details.
package objectstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileOpenRangeAndPut(t *testing.T) {
	store, err := New(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	input := filepath.Join(directory, "input.jsonl")
	if err := os.WriteFile(input, []byte("first\nsecond\nthird\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := store.OpenRange(context.Background(), fileURI(input), 6, 13)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if string(contents) != "second\n" {
		t.Fatalf("range = %q", contents)
	}

	output := filepath.Join(directory, "nested", "result.jsonl")
	if err := store.Put(context.Background(), fileURI(output), strings.NewReader("result\n")); err != nil {
		t.Fatal(err)
	}
	published, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(published) != "result\n" {
		t.Fatalf("output = %q", published)
	}
}

func TestRejectsInvalidURIsAndRanges(t *testing.T) {
	store, err := New(context.Background(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, uri := range []string{"", "https://example.com/a", "file://server/a", "file:///", "s3:///key", "s3://bucket/", "s3://bucket/key?version=1"} {
		if _, err := store.Open(context.Background(), uri); err == nil {
			t.Errorf("Open(%q) succeeded", uri)
		}
	}
	if _, err := store.Open(context.Background(), "s3://bucket/key"); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("unconfigured S3 error = %v", err)
	}
	if _, err := store.OpenRange(context.Background(), "file:///unused", 2, 2); err == nil {
		t.Fatal("accepted empty range")
	}
	if _, err := New(context.Background(), Config{Endpoint: "http://127.0.0.1:9000"}); err == nil {
		t.Fatal("accepted endpoint without region")
	}
}

func TestS3RangeAndPutUseConfiguredEndpoint(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	input := []byte("zero-one-two")
	var published string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/mill-input/records.jsonl":
			if request.Header.Get("Range") != "bytes=5-7" {
				t.Errorf("Range = %q", request.Header.Get("Range"))
			}
			response.Header().Set("Content-Length", "3")
			response.WriteHeader(http.StatusPartialContent)
			_, _ = response.Write(input[5:8])
		case request.Method == http.MethodPut && request.URL.Path == "/mill-output/result.jsonl":
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Error(err)
			}
			published = string(body)
			response.WriteHeader(http.StatusOK)
		default:
			http.Error(response, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	store, err := New(context.Background(), Config{Region: "us-east-1", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	rangeReader, err := store.OpenRange(context.Background(), "s3://mill-input/records.jsonl", 5, 8)
	if err != nil {
		t.Fatal(err)
	}
	rangeContents, err := io.ReadAll(rangeReader)
	if err != nil {
		t.Fatal(err)
	}
	if err := rangeReader.Close(); err != nil {
		t.Fatal(err)
	}
	if string(rangeContents) != "one" {
		t.Fatalf("range = %q", rangeContents)
	}
	if err := store.Put(context.Background(), "s3://mill-output/result.jsonl", strings.NewReader("done\n")); err != nil {
		t.Fatal(err)
	}
	if published != "done\n" {
		t.Fatalf("published = %q", published)
	}
}

func fileURI(filename string) string {
	return (&url.URL{Scheme: "file", Path: filename}).String()
}
