package job

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileManifestLoader(t *testing.T) {
	contents := []byte(`{
		"version": 1,
		"shards": [
			{"uri": "file:///data/shard-000.json"},
			{"uri": "file:///data/parts/../shard-001.json"}
		]
	}`)
	filename := filepath.Join(t.TempDir(), "manifest with spaces.json")
	if err := os.WriteFile(filename, contents, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	manifestURI := (&url.URL{Scheme: "file", Path: filename}).String()

	manifest, err := (FileManifestLoader{}).Load(context.Background(), manifestURI)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.Version != manifestVersion {
		t.Errorf("version = %d, want %d", manifest.Version, manifestVersion)
	}
	if len(manifest.Shards) != 2 {
		t.Fatalf("shard count = %d, want 2", len(manifest.Shards))
	}
	if got := manifest.Shards[1].URI; got != "file:///data/shard-001.json" {
		t.Errorf("normalized shard URI = %q, want %q", got, "file:///data/shard-001.json")
	}
	digest := sha256.Sum256(contents)
	if want := hex.EncodeToString(digest[:]); manifest.SHA256 != want {
		t.Errorf("SHA-256 = %q, want %q", manifest.SHA256, want)
	}
}

func TestFileManifestLoaderRejectsMissingFile(t *testing.T) {
	_, err := (FileManifestLoader{}).Load(context.Background(), "file:///definitely/not/present/manifest.json")
	var validationError *ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("error = %v, want ValidationError", err)
	}
}

func TestDecodeManifestRejectsInvalidDocuments(t *testing.T) {
	tests := map[string]string{
		"malformed JSON":        `{`,
		"multiple values":       `{"version":1,"shards":[{"uri":"file:///data/a"}]} {}`,
		"unknown field":         `{"version":1,"shards":[{"uri":"file:///data/a"}],"extra":true}`,
		"unsupported version":   `{"version":2,"shards":[{"uri":"file:///data/a"}]}`,
		"empty shard list":      `{"version":1,"shards":[]}`,
		"unsupported shard URI": `{"version":1,"shards":[{"uri":"s3://bucket/a"}]}`,
		"shard directory":       `{"version":1,"shards":[{"uri":"file:///data/a/"}]}`,
	}

	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeManifest([]byte(document)); err == nil {
				t.Fatal("decode manifest succeeded, want an error")
			}
		})
	}
}

func TestDecodeManifestEnforcesShardLimit(t *testing.T) {
	shard := `{"uri":"file:///data/a"}`
	document := `{"version":1,"shards":[` + strings.Repeat(shard+",", maxManifestShards) + shard + `]}`
	if _, err := decodeManifest([]byte(document)); err == nil {
		t.Fatal("decode oversized shard list succeeded, want an error")
	}
}
