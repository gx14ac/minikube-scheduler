package scheduler

import (
	"k8s.io/kube-scheduler/config/v1beta2"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
)

func DefaultSchedulerConfig() (*v1beta2.KubeSchedulerConfiguration, error) {
	var versionedCfg v1beta2.KubeSchedulerConfiguration
	scheme.Scheme.Default(&versionedCfg)

	return &versionedCfg, nil
}
