// This file tests backend-independent Store configuration and dispatch.

package objectstore_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/objectstore"
)

func TestEveryPublicOperationUsesTheSameURIValidation(t *testing.T) {
	store := newFileStore(t)
	invalidURIs := []string{
		"https://example.com/input.jsonl",
		"file://host/input.jsonl",
		"file:///",
		"s3:///input.jsonl",
		"s3://bucket/",
		"s3://bucket/input.jsonl?version=1",
	}
	for _, uri := range invalidURIs {
		t.Run(uri, func(t *testing.T) {
			if _, err := store.Open(context.Background(), uri); err == nil {
				t.Fatal("Open accepted an invalid URI")
			}
			if _, err := store.OpenRange(
				context.Background(), uri, 0, 1,
			); err == nil {
				t.Fatal("OpenRange accepted an invalid URI")
			}
			if err := store.Put(
				context.Background(),
				uri,
				strings.NewReader("result"),
			); err == nil {
				t.Fatal("Put accepted an invalid URI")
			}
		})
	}
}

func TestStoreRejectsInvalidConfigurationAndRanges(t *testing.T) {
	if _, err := objectstore.New(context.Background(), objectstore.Config{
		Endpoint: "http://127.0.0.1:9000",
	}); err == nil {
		t.Fatal("New accepted an endpoint without a region")
	}

	store := newFileStore(t)
	for _, byteRange := range [][2]int64{{-1, 1}, {2, 2}, {3, 2}} {
		if _, err := store.OpenRange(
			context.Background(),
			"file:///unused",
			byteRange[0],
			byteRange[1],
		); err == nil {
			t.Fatalf("OpenRange accepted range %v", byteRange)
		}
	}
}

func TestFileOnlyStoreRejectsS3Operations(t *testing.T) {
	store := newFileStore(t)
	if _, err := store.Open(
		context.Background(),
		"s3://bucket/input.jsonl",
	); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("Open error = %v", err)
	}
	if _, err := store.OpenRange(
		context.Background(),
		"s3://bucket/input.jsonl",
		0,
		1,
	); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("OpenRange error = %v", err)
	}
	if err := store.Put(
		context.Background(),
		"s3://bucket/output.jsonl",
		strings.NewReader("result"),
	); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("Put error = %v", err)
	}
}

func TestConfiguredStoreRoutesFilesWithoutContactingS3(t *testing.T) {
	setTestAWSCredentials(t)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		_ *http.Request,
	) {
		requests++
		http.Error(response, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	store, err := objectstore.New(context.Background(), objectstore.Config{
		Region:   "us-east-1",
		Endpoint: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "nested", "result.jsonl")
	if err := store.Put(
		context.Background(),
		fileURI(output),
		strings.NewReader("local\n"),
	); err != nil {
		t.Fatal(err)
	}
	assertObjectContents(t, store, fileURI(output), "local\n")
	reader, err := store.OpenRange(
		context.Background(), fileURI(output), 0, 5,
	)
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
	if string(contents) != "local" {
		t.Fatalf("range = %q, want local", contents)
	}
	if requests != 0 {
		t.Fatalf("file operations sent %d S3 requests", requests)
	}
}

func newFileStore(t *testing.T) *objectstore.Store {
	t.Helper()
	store, err := objectstore.New(context.Background(), objectstore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func setTestAWSCredentials(t *testing.T) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func assertObjectContents(
	t *testing.T,
	store *objectstore.Store,
	uri, expected string,
) {
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
