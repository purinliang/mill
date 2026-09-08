// This file implements local file reads, ranged reads, and atomic writes.

package objectstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func openFile(filename string) (io.ReadCloser, error) {
	return openRegularFile(filename)
}

func openFileRange(
	filename string,
	start, end int64,
) (io.ReadCloser, error) {
	file, err := openRegularFile(filename)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if end > info.Size() {
		_ = file.Close()
		return nil, fmt.Errorf(
			"byte range ends at %d beyond object size %d",
			end,
			info.Size(),
		)
	}
	return &sectionReadCloser{
		Reader: io.NewSectionReader(file, start, end-start),
		closer: file,
	}, nil
}

func openRegularFile(filename string) (*os.File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, errors.New("file URI must refer to a regular file")
	}
	return file, nil
}

func putFile(filename string, body io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	temporary, err := os.CreateTemp(
		filepath.Dir(filename),
		".mill-output-*.tmp",
	)
	if err != nil {
		return fmt.Errorf("create temporary output: %w", err)
	}
	temporaryFilename := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryFilename)
		}
	}()
	if _, err := io.Copy(temporary, body); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close output: %w", err)
	}
	if err := os.Rename(temporaryFilename, filename); err != nil {
		return fmt.Errorf("publish output: %w", err)
	}
	committed = true
	return nil
}
