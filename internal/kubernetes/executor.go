// Package kubernetes executes Mill attempts as native Jobs on a configured node.
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
	"k8s.io/client-go/tools/clientcmd"

	"github.com/purinliang/mill/internal/coordinator"
	"github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/workload"
)

type Config struct {
	Context   string
	Namespace string
	Node      string
	LocalRoot string
	NodeRoot  string
}

type Executor struct {
	jobs   batchclient.JobInterface
	config Config
}

func New(config Config) (*Executor, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: config.Context})
	restConfig, err := loader.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes config: %w", err)
	}
	restConfig.Timeout = 10 * time.Second
	client, err := batchclient.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	return &Executor{jobs: client.Jobs(config.Namespace), config: config}, nil
}

func (c Config) validate() error {
	if c.Context == "" || c.Namespace == "" || c.Node == "" {
		return errors.New("Kubernetes context, namespace, and node must be explicit")
	}
	for _, root := range []string{c.LocalRoot, c.NodeRoot} {
		if !filepath.IsAbs(root) || filepath.Clean(root) == "/" || filepath.Clean(root) != root {
			return errors.New("Kubernetes local/node roots must be clean absolute directories other than /")
		}
	}
	return nil
}

// Reconcile recovers the create/record crash window by a stable Job name.
// Running attempts never recreate missing resources: that could rerun work
// while a deleted Job's Pods are still terminating.
func (e *Executor) Reconcile(ctx context.Context, claimed job.ClaimedAttempt) (coordinator.Observation, error) {
	name := "mill-" + claimed.Attempt.ID
	external, err := e.jobs.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) && claimed.Attempt.State == job.AttemptStateStarting {
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

func (e *Executor) manifest(claimed job.ClaimedAttempt) (*batchv1.Job, error) {
	input, err := e.relativeURI(claimed.InputURI, "input")
	if err != nil {
		return nil, err
	}
	output, err := e.relativeURI(claimed.OutputURI, "output")
	if err != nil {
		return nil, err
	}
	args, err := (workload.Invocation{
		JobID: claimed.Attempt.JobID, TaskID: claimed.Attempt.TaskID,
		ShardIndex:     claimed.ShardIndex,
		InputURI:       (&url.URL{Scheme: "file", Path: filepath.Join("/data", input)}).String(),
		InputStartByte: claimed.InputStartByte, InputEndByte: claimed.InputEndByte,
		OutputURI:      (&url.URL{Scheme: "file", Path: filepath.Join("/output", output)}).String(),
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
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "mill-" + claimed.Attempt.ID, Namespace: e.config.Namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			Parallelism: &one, Completions: &one, BackoffLimit: &zero, ActiveDeadlineSeconds: &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: &no,
					NodeSelector:                 map[string]string{"kubernetes.io/hostname": e.config.Node},
					SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &yes, RunAsUser: &user, RunAsGroup: &user,
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					Containers: []corev1.Container{{
						Name: "workload", Image: claimed.Executable.Image, ImagePullPolicy: corev1.PullNever, Args: args,
						SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes,
							Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("128Mi")}},
						VolumeMounts: []corev1.VolumeMount{{Name: "input", MountPath: "/data", ReadOnly: true}, {Name: "output", MountPath: "/output"}},
					}},
					Volumes: []corev1.Volume{
						{Name: "input", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: filepath.Join(e.config.NodeRoot, "input"), Type: &directory}}},
						{Name: "output", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: filepath.Join(e.config.NodeRoot, "output"), Type: &directory}}},
					},
				},
			},
		},
	}, nil
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
