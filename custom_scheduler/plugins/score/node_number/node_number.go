package nodenumber

import (
	"context"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type NodeNumber struct{}

var _ framework.ScorePlugin = &NodeNumber{}

const Name = "NodeNumber"

func (pl *NodeNumber) Name() string {
	return Name
}

// Nodeの数字のPrefixとPodの数字のPrefixが一致するものが１０点得られる
//
func (pl *NodeNumber) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	podNameLastChar := pod.Name[len(pod.Name)-1:]
	podNum, err := strconv.Atoi(podNameLastChar)
	if err != nil {
		return 0, nil
	}

	nodeNameLastChar := nodeName[len(nodeName)-1:]
	nodeNum, err := strconv.Atoi(nodeNameLastChar)
	if err != nil {
		return 0, nil
	}

	if podNum == nodeNum {
		return 10, nil
	}

	return 0, nil
}

func (pl *NodeNumber) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

func New(_ runtime.Object, _ framework.Handle) (framework.Plugin, error) {
	return &NodeNumber{}, nil
}
