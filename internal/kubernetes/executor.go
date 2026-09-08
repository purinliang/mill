// Package kubernetes executes Mill attempts as native Jobs.
package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	batchclient "k8s.io/client-go/kubernetes/typed/batch/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/purinliang/mill/internal/coordinator"
	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/workload"
)

type Config struct {
	Context             string
	InCluster           bool
	Namespace           string
	Node                string
	LocalRoot           string
	NodeRoot            string
	S3Region            string
	S3Endpoint          string
	S3CredentialsSecret string
}

type Executor struct {
	jobs   batchclient.JobInterface
	config Config
}

func New(config Config) (*Executor, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	restConfig, err := config.restConfig()
	if err != nil {
		return nil, err
	}
	restConfig.Timeout = 10 * time.Second
	client, err := batchclient.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	return &Executor{jobs: client.Jobs(config.Namespace), config: config}, nil
}

func (c Config) validate() error {
	if c.Namespace == "" {
		return errors.New("Kubernetes namespace must be explicit")
	}
	if (c.Context == "" && !c.InCluster) || (c.Context != "" && c.InCluster) {
		return errors.New("configure exactly one Kubernetes client mode: context or in-cluster")
	}
	localFields := 0
	for _, value := range []string{c.Node, c.LocalRoot, c.NodeRoot} {
		if value != "" {
			localFields++
		}
	}
	if localFields != 0 && localFields != 3 {
		return errors.New("Kubernetes node and local/node roots must be configured together")
	}
	for _, root := range []string{c.LocalRoot, c.NodeRoot} {
		if root == "" {
			continue
		}
		if !filepath.IsAbs(root) || filepath.Clean(root) == "/" || filepath.Clean(root) != root {
			return errors.New("Kubernetes local/node roots must be clean absolute directories other than /")
		}
	}
	if c.S3Endpoint != "" && c.S3Region == "" {
		return errors.New("Kubernetes workload S3 region is required with a custom endpoint")
	}
	return nil
}

func (c Config) restConfig() (*rest.Config, error) {
	if c.InCluster {
		config, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("load in-cluster Kubernetes config: %w", err)
		}
		return config, nil
	}
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: c.Context})
	config, err := loader.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig context %q: %w", c.Context, err)
	}
	return config, nil
}

// Reconcile recovers the create/record crash window by a stable Job name.
// Running attempts never recreate missing resources: that could rerun work
// while a deleted Job's Pods are still terminating.
func (e *Executor) Reconcile(ctx context.Context, claimed execution.ClaimedAttempt) (coordinator.Observation, error) {
	name := "mill-" + claimed.Attempt.ID
	external, err := e.jobs.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && claimed.Attempt.State == execution.AttemptStateStarting {
		manifest, buildErr := e.manifest(claimed)
		if buildErr != nil {
			return coordinator.Observation{Failure: buildErr.Error()}, nil
		}
		external, err = e.jobs.Create(ctx, manifest, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			external, err = e.jobs.Get(ctx, name, metav1.GetOptions{})
		}
		if apierrors.IsInvalid(err) {
			return coordinator.Observation{Failure: boundedMessage(err.Error())}, nil
		}
	}
	if err != nil {
		return coordinator.Observation{}, fmt.Errorf("observe/create Kubernetes Job %s: %w", name, err)
	}
	if external.Labels["mill.dev/attempt-id"] != claimed.Attempt.ID ||
		external.Labels["mill.dev/job-id"] != claimed.Attempt.JobID ||
		external.Labels["mill.dev/task-id"] != claimed.Attempt.TaskID {
		return coordinator.Observation{}, fmt.Errorf("Kubernetes Job %s identity does not match attempt", name)
	}
	externalID := string(external.UID)
	if externalID == "" || (claimed.Attempt.ExternalID != "" && claimed.Attempt.ExternalID != externalID) {
		return coordinator.Observation{}, fmt.Errorf("Kubernetes Job %s UID is missing or changed", name)
	}
	observed := coordinator.Observation{ExternalID: externalID}
	for _, condition := range external.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case batchv1.JobComplete:
			observed.Completed = true
		case batchv1.JobFailed:
			observed.Failure = boundedMessage(condition.Reason + ": " + condition.Message)
		}
	}
	return observed, nil
}

func (e *Executor) manifest(claimed execution.ClaimedAttempt) (*batchv1.Job, error) {
	if claimed.Resources.CPURequestMillis < 1 ||
		claimed.Resources.CPULimitMillis < claimed.Resources.CPURequestMillis ||
		claimed.Resources.MemoryRequestBytes < 1 ||
		claimed.Resources.MemoryLimitBytes < claimed.Resources.MemoryRequestBytes {
		return nil, errors.New("claimed attempt has invalid workload resources")
	}
	inputURI, inputLocal, err := e.workloadURI(claimed.InputURI, "input")
	if err != nil {
		return nil, err
	}
	outputURI, outputLocal, err := e.workloadURI(claimed.OutputURI, "output")
	if err != nil {
		return nil, err
	}
	args, err := (workload.Invocation{
		JobID: claimed.Attempt.JobID, TaskID: claimed.Attempt.TaskID,
		ShardIndex:     claimed.ShardIndex,
		InputURI:       inputURI,
		InputStartByte: claimed.InputStartByte, InputEndByte: claimed.InputEndByte,
		OutputURI:      outputURI,
		ExecutableArgs: claimed.Executable.Args,
	}).CommandArgs()
	if err != nil {
		return nil, err
	}
	labels := map[string]string{
		"app.kubernetes.io/name": "mill", "mill.dev/job-id": claimed.Attempt.JobID,
		"mill.dev/task-id": claimed.Attempt.TaskID, "mill.dev/attempt-id": claimed.Attempt.ID,
	}
	zero, one := int32(0), int32(1)
	deadline, user := int64(300), int64(65532)
	yes, no := true, false
	directory := corev1.HostPathDirectory
	container := corev1.Container{
		Name: "workload", Image: claimed.Executable.Image, ImagePullPolicy: corev1.PullIfNotPresent, Args: args,
		SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes,
			Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(claimed.Resources.CPURequestMillis, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(claimed.Resources.MemoryRequestBytes, resource.BinarySI),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(claimed.Resources.CPULimitMillis, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(claimed.Resources.MemoryLimitBytes, resource.BinarySI),
			},
		},
	}
	pod := corev1.PodSpec{
		RestartPolicy:                corev1.RestartPolicyNever,
		AutomountServiceAccountToken: &no,
		SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &yes, RunAsUser: &user, RunAsGroup: &user,
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
	}
	if inputLocal || outputLocal {
		pod.NodeSelector = map[string]string{"kubernetes.io/hostname": e.config.Node}
	}
	if inputLocal {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "input", MountPath: "/data", ReadOnly: true})
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "input", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: filepath.Join(e.config.NodeRoot, "input"), Type: &directory}}})
	}
	if outputLocal {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "output", MountPath: "/output"})
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "output", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: filepath.Join(e.config.NodeRoot, "output"), Type: &directory}}})
	}
	if !inputLocal || !outputLocal {
		container.Env = append(container.Env, corev1.EnvVar{Name: "AWS_REGION", Value: e.config.S3Region})
		if e.config.S3Endpoint != "" {
			container.Env = append(container.Env, corev1.EnvVar{Name: "MILL_S3_ENDPOINT", Value: e.config.S3Endpoint})
		}
		if e.config.S3CredentialsSecret != "" {
			container.EnvFrom = append(container.EnvFrom, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: e.config.S3CredentialsSecret},
			}})
		}
	}
	pod.Containers = []corev1.Container{container}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "mill-" + claimed.Attempt.ID, Namespace: e.config.Namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			Parallelism: &one, Completions: &one, BackoffLimit: &zero, ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       pod,
			},
		},
	}, nil
}

func (e *Executor) workloadURI(raw, directory string) (string, bool, error) {
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return "", false, fmt.Errorf("parse %s URI: %w", directory, err)
	}
	if u.Scheme == "s3" {
		if e.config.S3Region == "" {
			return "", false, fmt.Errorf("%s uses S3 but Kubernetes workload S3 region is not configured", directory)
		}
		if u.Host == "" || strings.TrimPrefix(u.Path, "/") == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return "", false, fmt.Errorf("%s must use an absolute S3 object URI", directory)
		}
		return raw, false, nil
	}
	if e.config.Node == "" {
		return "", false, fmt.Errorf("%s uses a local file but Kubernetes local storage is not configured", directory)
	}
	relative, err := e.relativeURI(raw, directory)
	if err != nil {
		return "", false, err
	}
	mountRoot := "/data"
	if directory == "output" {
		mountRoot = "/output"
	}
	return (&url.URL{Scheme: "file", Path: filepath.Join(mountRoot, relative)}).String(), true, nil
}

func (e *Executor) relativeURI(raw, directory string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "file" || u.Host != "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !filepath.IsAbs(u.Path) {
		return "", fmt.Errorf("%s must use an absolute local file URI", directory)
	}
	root := filepath.Join(e.config.LocalRoot, directory)
	relative, err := filepath.Rel(root, u.Path)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, "../") {
		return "", fmt.Errorf("%s URI must be below %s", directory, root)
	}
	return relative, nil
}

func boundedMessage(value string) string {
	if len(value) > 4096 {
		return value[:4096]
	}
	return value
}
