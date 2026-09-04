package job

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
)

const (
	manifestVersion   = 1
	maxManifestBytes  = 4 << 20
	maxManifestShards = 10_000
)

type Manifest struct {
	Version int             `json:"version"`
	Shards  []ManifestShard `json:"shards"`
	SHA256  string          `json:"-"`
}

type ManifestShard struct {
	URI string `json:"uri"`
}

type ManifestLoader interface {
	Load(context.Context, string) (Manifest, error)
}

type FileManifestLoader struct{}

func (FileManifestLoader) Load(ctx context.Context, manifestURI string) (Manifest, error) {
	normalizedURI, err := normalizeManifestURI(manifestURI)
	if err != nil {
		return Manifest{}, &ValidationError{Field: "dataset.manifest_uri", Problem: err.Error()}
	}

	parsed, err := url.Parse(normalizedURI)
	if err != nil {
		return Manifest{}, fmt.Errorf("parse normalized manifest URI: %w", err)
	}
	filename, err := url.PathUnescape(parsed.EscapedPath())
	if err != nil {
		return Manifest{}, &ValidationError{Field: "dataset.manifest_uri", Problem: "contains an invalid escaped path"}
	}

	file, err := os.Open(filename)
	if err != nil {
		return Manifest{}, &ValidationError{Field: "dataset.manifest_uri", Problem: "cannot be opened"}
	}
	defer file.Close()

	contents, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil {
		return Manifest{}, fmt.Errorf("read dataset manifest: %w", err)
	}
	if len(contents) > maxManifestBytes {
		return Manifest{}, &ValidationError{Field: "dataset.manifest_uri", Problem: "manifest must be at most 4 MiB"}
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}

	manifest, err := decodeManifest(contents)
	if err != nil {
		return Manifest{}, err
	}
	digest := sha256.Sum256(contents)
	manifest.SHA256 = hex.EncodeToString(digest[:])
	return manifest, nil
}

func decodeManifest(contents []byte) (Manifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()

	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, &ValidationError{Field: "dataset.manifest_uri", Problem: "must contain a valid JSON manifest"}
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Manifest{}, &ValidationError{Field: "dataset.manifest_uri", Problem: "must contain exactly one JSON object"}
	}
	if manifest.Version != manifestVersion {
		return Manifest{}, &ValidationError{Field: "manifest.version", Problem: "must be 1"}
	}
	if len(manifest.Shards) == 0 {
		return Manifest{}, &ValidationError{Field: "manifest.shards", Problem: "must contain at least one shard"}
	}
	if len(manifest.Shards) > maxManifestShards {
		return Manifest{}, &ValidationError{Field: "manifest.shards", Problem: "must contain at most 10000 shards"}
	}

	for index := range manifest.Shards {
		normalizedURI, err := normalizeShardURI(manifest.Shards[index].URI)
		if err != nil {
			return Manifest{}, &ValidationError{
				Field:   fmt.Sprintf("manifest.shards[%d].uri", index),
				Problem: err.Error(),
			}
		}
		manifest.Shards[index].URI = normalizedURI
	}
	return manifest, nil
}

func validateMaterializedManifest(manifest Manifest) error {
	if manifest.Version != manifestVersion {
		return &ValidationError{Field: "manifest.version", Problem: "must be 1"}
	}
	if len(manifest.Shards) == 0 || len(manifest.Shards) > maxManifestShards {
		return &ValidationError{Field: "manifest.shards", Problem: "must contain between 1 and 10000 shards"}
	}
	decodedSHA256, err := hex.DecodeString(manifest.SHA256)
	if err != nil || len(decodedSHA256) != sha256.Size || manifest.SHA256 != strings.ToLower(manifest.SHA256) {
		return &ValidationError{Field: "manifest SHA-256", Problem: "must be 64 lowercase hexadecimal characters"}
	}
	for index, shard := range manifest.Shards {
		normalizedURI, err := normalizeShardURI(shard.URI)
		if err != nil || normalizedURI != shard.URI {
			problem := "must be a normalized absolute local file:// URI"
			if err != nil {
				problem = err.Error()
			}
			return &ValidationError{Field: fmt.Sprintf("manifest.shards[%d].uri", index), Problem: problem}
		}
	}
	return nil
}
