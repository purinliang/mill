// This file tests the public Job parallelism limits.
package job_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/job"
)

func TestValidateParallelismUsesDocumentedBounds(t *testing.T) {
	for _, parallelism := range []int{1, job.MaxParallelism} {
		if err := job.ValidateParallelism(parallelism); err != nil {
			t.Fatalf("ValidateParallelism(%d): %v", parallelism, err)
		}
	}

	for _, parallelism := range []int{0, job.MaxParallelism + 1} {
		err := job.ValidateParallelism(parallelism)
		var validationError *job.ValidationError
		if !errors.As(err, &validationError) {
			t.Fatalf(
				"ValidateParallelism(%d) error = %T, want ValidationError",
				parallelism,
				err,
			)
		}
		if validationError.Field != "parallelism" || !strings.Contains(
			validationError.Problem,
			strconv.Itoa(job.MaxParallelism),
		) {
			t.Fatalf("validation error = %+v", validationError)
		}
	}
}
