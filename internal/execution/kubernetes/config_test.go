// This file tests Kubernetes runtime configuration validation.
package kubernetes_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/execution/kubernetes"
)

func TestNewRejectsInvalidConfigurationBeforeContactingKubernetes(
	t *testing.T,
) {
	tests := []struct {
		name   string
		config kubernetes.Config
	}{
		{
			name:   "missing namespace",
			config: kubernetes.Config{Context: "unused"},
		},
		{
			name:   "missing client mode",
			config: kubernetes.Config{Namespace: "default"},
		},
		{
			name: "two client modes",
			config: kubernetes.Config{
				Namespace: "default", Context: "unused", InCluster: true,
			},
		},
		{
			name: "partial local storage",
			config: kubernetes.Config{
				Namespace: "default", Context: "unused", Node: "node-1",
			},
		},
		{
			name: "unsafe local root",
			config: kubernetes.Config{
				Namespace: "default",
				Context:   "unused",
				Node:      "node-1",
				LocalRoot: "/",
				NodeRoot:  "/data",
			},
		},
		{
			name: "endpoint without region",
			config: kubernetes.Config{
				Namespace:  "default",
				Context:    "unused",
				S3Endpoint: "http://s3.example",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := kubernetes.New(test.config); err == nil {
				t.Fatal("New accepted invalid configuration")
			}
		})
	}
}

func TestNewReportsKubernetesCredentialLoadingFailures(t *testing.T) {
	t.Run("missing kubeconfig context", func(t *testing.T) {
		t.Setenv(
			"KUBECONFIG",
			filepath.Join(t.TempDir(), "missing-kubeconfig"),
		)
		_, err := kubernetes.New(kubernetes.Config{
			Context: "missing", Namespace: "default",
		})
		if err == nil || !strings.Contains(err.Error(), "load kubeconfig") {
			t.Fatalf("New error = %v", err)
		}
	})

	t.Run("outside a Kubernetes Pod", func(t *testing.T) {
		t.Setenv("KUBERNETES_SERVICE_HOST", "")
		t.Setenv("KUBERNETES_SERVICE_PORT", "")
		_, err := kubernetes.New(kubernetes.Config{
			InCluster: true, Namespace: "default",
		})
		if err == nil || !strings.Contains(err.Error(), "in-cluster") {
			t.Fatalf("New error = %v", err)
		}
	})
}
