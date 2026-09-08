// This file tests S3-compatible object reads, ranges, writes, and failures.

package objectstore_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/objectstore"
)

func TestS3OpenRangeAndPutUseConfiguredEndpoint(t *testing.T) {
	setTestAWSCredentials(t)
	const contents = "zero-one-two"
	var published string
	server := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		request *http.Request,
	) {
		switch {
		case request.Method == http.MethodGet &&
			request.URL.Path == "/mill-input/records.jsonl" &&
			request.Header.Get("Range") == "":
			response.Header().Set("Content-Length", "12")
			_, _ = io.WriteString(response, contents)
		case request.Method == http.MethodGet &&
			request.URL.Path == "/mill-input/records.jsonl":
			if request.Header.Get("Range") != "bytes=5-7" {
				t.Errorf("Range = %q", request.Header.Get("Range"))
			}
			response.Header().Set("Content-Length", "3")
			response.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(response, contents[5:8])
		case request.Method == http.MethodPut &&
			request.URL.Path == "/mill-output/result.jsonl":
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

	store, err := objectstore.New(context.Background(), objectstore.Config{
		Region:   "us-east-1",
		Endpoint: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertObjectContents(
		t,
		store,
		"s3://mill-input/records.jsonl",
		contents,
	)
	rangeReader, err := store.OpenRange(
		context.Background(),
		"s3://mill-input/records.jsonl",
		5,
		8,
	)
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
		t.Fatalf("range = %q, want one", rangeContents)
	}
	if err := store.Put(
		context.Background(),
		"s3://mill-output/result.jsonl",
		strings.NewReader("done\n"),
	); err != nil {
		t.Fatal(err)
	}
	if published != "done\n" {
		t.Fatalf("published = %q", published)
	}
}

func TestS3OperationsExposeRemoteFailures(t *testing.T) {
	setTestAWSCredentials(t)
	server := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		_ *http.Request,
	) {
		http.Error(response, "denied", http.StatusForbidden)
	}))
	defer server.Close()

	store, err := objectstore.New(context.Background(), objectstore.Config{
		Region:   "us-east-1",
		Endpoint: server.URL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Open(
		context.Background(),
		"s3://bucket/input.jsonl",
	); err == nil {
		t.Fatal("Open hid an S3 failure")
	}
	if _, err := store.OpenRange(
		context.Background(),
		"s3://bucket/input.jsonl",
		0,
		1,
	); err == nil {
		t.Fatal("OpenRange hid an S3 failure")
	}
	if err := store.Put(
		context.Background(),
		"s3://bucket/output.jsonl",
		strings.NewReader("result"),
	); err == nil {
		t.Fatal("Put hid an S3 failure")
	}
}
