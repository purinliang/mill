// This file parses object URIs into fully validated backend locations.

package objectstore

import (
	"errors"
	"net/url"
	"path/filepath"
	"strings"
)

type location struct {
	scheme   string
	filename string
	bucket   string
	key      string
}

func parseURI(raw string) (location, error) {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.User != nil || parsed.Opaque != "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return location{}, errors.New(
			"object URI must be an absolute file:// or s3:// URI " +
				"without a query or fragment",
		)
	}
	switch parsed.Scheme {
	case "file":
		if parsed.Host != "" {
			return location{}, errors.New("file URI must not contain a host")
		}
		filename, err := url.PathUnescape(parsed.EscapedPath())
		if err != nil || !filepath.IsAbs(filename) ||
			filepath.Clean(filename) == "/" {
			return location{}, errors.New(
				"file URI must contain an absolute path other than /",
			)
		}
		return location{
			scheme:   "file",
			filename: filepath.Clean(filename),
		}, nil
	case "s3":
		if parsed.Host == "" || parsed.Port() != "" ||
			strings.Contains(parsed.Host, ":") {
			return location{}, errors.New(
				"S3 URI must contain one bucket name",
			)
		}
		key, err := url.PathUnescape(
			strings.TrimPrefix(parsed.EscapedPath(), "/"),
		)
		if err != nil || key == "" || strings.HasSuffix(parsed.Path, "/") {
			return location{}, errors.New("S3 URI must contain an object key")
		}
		return location{
			scheme: "s3",
			bucket: parsed.Host,
			key:    key,
		}, nil
	default:
		return location{}, errors.New("object URI scheme must be file or s3")
	}
}
