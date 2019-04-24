/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// TODO: Refactor common part of functions in this file for generic object kinds.

package utils

import (
	"fmt"
	"time"

	apps "k8s.io/api/apps/v1"
	batch "k8s.io/api/batch/v1"
	"k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
)

const (
	// Parameters for retrying with exponential backoff.
	retryBackoffInitialDuration = 100 * time.Millisecond
	retryBackoffFactor          = 3
	retryBackoffJitter          = 0
	retryBackoffSteps           = 6
)

// RetryWithExponentialBackOff is a utility for retrying the given function with exponential backoff.
func RetryWithExponentialBackOff(fn wait.ConditionFunc) error {
	backoff := wait.Backoff{
		Duration: retryBackoffInitialDuration,
		Factor:   retryBackoffFactor,
		Jitter:   retryBackoffJitter,
		Steps:    retryBackoffSteps,
	}
	return wait.ExponentialBackoff(backoff, fn)
}

// IsRetryableAPIError indicates whether an error is transient or explicitly confirms that we should retry.
func IsRetryableAPIError(err error) bool {
	// These errors may indicate a transient error that we can retry in tests.
	if apierrs.IsInternalError(err) || apierrs.IsTimeout(err) || apierrs.IsServerTimeout(err) ||
		apierrs.IsTooManyRequests(err) || utilnet.IsProbableEOF(err) || utilnet.IsConnectionReset(err) {
		return true
	}
	// If the error sends the Retry-After header, we respect it as an explicit confirmation we should retry.
	if _, shouldRetry := apierrs.SuggestsClientDelay(err); shouldRetry {
		return true
	}
	return false
}

// CreatePodWithRetries retries creation of a Pod based around the given inputs using exponential backoff.
func CreatePodWithRetries(c clientset.Interface, namespace string, obj *v1.Pod) error {
	if obj == nil {
		return fmt.Errorf("Object provided to create is empty")
	}
	createFunc := func() (bool, error) {
		_, err := c.CoreV1().Pods(namespace).Create(obj)
		if err == nil || apierrs.IsAlreadyExists(err) {
			return true, nil
		}
		if IsRetryableAPIError(err) {
			return false, nil
		}
		return false, fmt.Errorf("Failed to create object with non-retriable error: %v", err)
	}
	return RetryWithExponentialBackOff(createFunc)
}

// CreateRCWithRetries retries creation of a ReplicationController based around the given inputs using exponential
// backoff.
func CreateRCWithRetries(c clientset.Interface, namespace string, obj *v1.ReplicationController) error {
	if obj == nil {
		return fmt.Errorf("Object provided to create is empty")
	}
	createFunc := func() (bool, error) {
		_, err := c.CoreV1().ReplicationControllers(namespace).Create(obj)
		if err == nil || apierrs.IsAlreadyExists(err) {
			return true, nil
		}
		if IsRetryableAPIError(err) {
			return false, nil
		}
		return false, fmt.Errorf("Failed to create object with non-retriable error: %v", err)
	}
	return RetryWithExponentialBackOff(createFunc)
}

// CreateReplicaSetWithRetries retries creation of a ReplicaSet based around the given inputs using exponential
// backoff.
func CreateReplicaSetWithRetries(c clientset.Interface, namespace string, obj *apps.ReplicaSet) error {
	if obj == nil {
		return fmt.Errorf("Object provided to create is empty")
	}
	createFunc := func() (bool, error) {
		_, err := c.AppsV1().ReplicaSets(namespace).Create(obj)
		if err == nil || apierrs.IsAlreadyExists(err) {
			return true, nil
		}
		if IsRetryableAPIError(err) {
			return false, nil
		}
		return false, fmt.Errorf("Failed to create object with non-retriable error: %v", err)
	}
	return RetryWithExponentialBackOff(createFunc)
}

// CreateDeploymentWithRetries retries creation of a Deployment based around the given inputs using exponential
// backoff.
func CreateDeploymentWithRetries(c clientset.Interface, namespace string, obj *apps.Deployment) error {
	if obj == nil {
		return fmt.Errorf("Object provided to create is empty")
	}
	createFunc := func() (bool, error) {
		_, err := c.AppsV1().Deployments(namespace).Create(obj)
		if err == nil || apierrs.IsAlreadyExists(err) {
			return true, nil
		}
		if IsRetryableAPIError(err) {
			return false, nil
		}
		return false, fmt.Errorf("Failed to create object with non-retriable error: %v", err)
	}
	return RetryWithExponentialBackOff(createFunc)
}

// CreateDaemonSetWithRetries retries creation of a DaemonSet based around the given inputs using exponential backoff.
func CreateDaemonSetWithRetries(c clientset.Interface, namespace string, obj *apps.DaemonSet) error {
	if obj == nil {
		return fmt.Errorf("Object provided to create is empty")
	}
	createFunc := func() (bool, error) {
		_, err := c.AppsV1().DaemonSets(namespace).Create(obj)
		if err == nil || apierrs.IsAlreadyExists(err) {
			return true, nil
		}
		if IsRetryableAPIError(err) {
			return false, nil
		}
		return false, fmt.Errorf("Failed to create object with non-retriable error: %v", err)
	}
	return RetryWithExponentialBackOff(createFunc)
}

// CreateJobWithRetries retries creation of a Job based around the given inputs using exponential backoff.
func CreateJobWithRetries(c clientset.Interface, namespace string, obj *batch.Job) error {
	if obj == nil {
		return fmt.Errorf("Object provided to create is empty")
	}
	createFunc := func() (bool, error) {
		_, err := c.BatchV1().Jobs(namespace).Create(obj)
		if err == nil || apierrs.IsAlreadyExists(err) {
			return true, nil
		}
		if IsRetryableAPIError(err) {
			return false, nil
		}
		return false, fmt.Errorf("Failed to create object with non-retriable error: %v", err)
	}
	return RetryWithExponentialBackOff(createFunc)
}

// CreateSecretWithRetries retries creation of a Secret based around the given inputs using exponential backoff.
func CreateSecretWithRetries(c clientset.Interface, namespace string, obj *v1.Secret) error {
	if obj == nil {
		return fmt.Errorf("Object provided to create is empty")
	}
	createFunc := func() (bool, error) {
		_, err := c.CoreV1().Secrets(namespace).Create(obj)
		if err == nil || apierrs.IsAlreadyExists(err) {
			return true, nil
		}
		if IsRetryableAPIError(err) {
			return false, nil
		}
		return false, fmt.Errorf("Failed to create object with non-retriable error: %v", err)
	}
	return RetryWithExponentialBackOff(createFunc)
}

// CreateConfigMapWithRetries retries creation of a ConfigMap based around the given inputs using exponential backoff.
func CreateConfigMapWithRetries(c clientset.Interface, namespace string, obj *v1.ConfigMap) error {
	if obj == nil {
		return fmt.Errorf("Object provided to create is empty")
	}
	createFunc := func() (bool, error) {
		_, err := c.CoreV1().ConfigMaps(namespace).Create(obj)
		if err == nil || apierrs.IsAlreadyExists(err) {
			return true, nil
		}
		if IsRetryableAPIError(err) {
			return false, nil
		}
		return false, fmt.Errorf("Failed to create object with non-retriable error: %v", err)
	}
	return RetryWithExponentialBackOff(createFunc)
}

// CreateServiceWithRetries retries creation of a Service based around the given inputs using exponential backoff.
func CreateServiceWithRetries(c clientset.Interface, namespace string, obj *v1.Service) error {
	if obj == nil {
		return fmt.Errorf("Object provided to create is empty")
	}
	createFunc := func() (bool, error) {
		_, err := c.CoreV1().Services(namespace).Create(obj)
		if err == nil || apierrs.IsAlreadyExists(err) {
			return true, nil
		}
		if IsRetryableAPIError(err) {
			return false, nil
		}
		return false, fmt.Errorf("Failed to create object with non-retriable error: %v", err)
	}
	return RetryWithExponentialBackOff(createFunc)
}

// CreateResourceQuotaWithRetries retries creation of a ResourceQuota based around the given inputs using exponential
// backoff.
func CreateResourceQuotaWithRetries(c clientset.Interface, namespace string, obj *v1.ResourceQuota) error {
	if obj == nil {
		return fmt.Errorf("Object provided to create is empty")
	}
	createFunc := func() (bool, error) {
		_, err := c.CoreV1().ResourceQuotas(namespace).Create(obj)
		if err == nil || apierrs.IsAlreadyExists(err) {
			return true, nil
		}
		if IsRetryableAPIError(err) {
			return false, nil
		}
		return false, fmt.Errorf("Failed to create object with non-retriable error: %v", err)
	}
	return RetryWithExponentialBackOff(createFunc)
}
