package nodenumber

import (
	"context"
	"errors"
	"strconv"
	"time"

	waitingpod "github.com/shintard/minikube-scheduler/custom_scheduler/waiting_pod"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type NodeNumber struct {
	wt waitingpod.Handle
}

var _ framework.ScorePlugin = &NodeNumber{}
var _ framework.PreScorePlugin = &NodeNumber{}
var _ framework.PermitPlugin = &NodeNumber{}

const Name = "NodeNumber"
const preScoreStateKey = "PreScore" + Name

type preScoreState struct {
	podSuffixNumber int
}

func (s *preScoreState) Clone() framework.StateData {
	return s
}

func (pl *NodeNumber) Name() string {
	return Name
}

// Nodeの数字のPrefixとPodの数字のPrefixが一致するものが１０点得られる
//
func (pl *NodeNumber) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	score, err := state.Read(preScoreStateKey)
	if err != nil {
		return 0, framework.AsStatus(err)
	}

	s, ok := score.(*preScoreState)
	if !ok {
		return 0, framework.AsStatus(errors.New("failed to convert type pre score state"))
	}

	nodeNameLastChar := nodeName[len(nodeName)-1:]

	nodeNum, err := strconv.Atoi(nodeNameLastChar)
	if err != nil {
		return 0, nil
	}

	if s.podSuffixNumber == nodeNum {
		return 10, nil
	}

	return 0, nil
}

func (pl *NodeNumber) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// PreScoreで事前にPodの名前の末尾の数字を取得してScoreではCycleStateから取得するよう
//
func (pl *NodeNumber) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	podNameLastChar := pod.Name[len(pod.Name)-1:]
	podNum, err := strconv.Atoi(podNameLastChar)
	if err != nil {
		return nil
	}

	s := &preScoreState{
		podSuffixNumber: podNum,
	}
	state.Write(preScoreStateKey, s)

	return nil
}

// nodeの末尾の数字秒待ってから、StausをSuccessにする
//
func (pl *NodeNumber) Permit(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	nodeNameLastChart := nodeName[len(nodeName)-1:]

	nodeNum, err := strconv.Atoi(nodeNameLastChart)
	if err != nil {
		return nil, 0
	}

	time.AfterFunc(time.Duration(nodeNum)*time.Second, func() {
		wp := pl.wt.GetWaitingPod(p.GetUID())
		wp.Allow(pl.Name())
	})

	timeOut := time.Duration(10) * time.Second
	return framework.NewStatus(framework.Wait, ""), timeOut
}

func New(_ runtime.Object, h waitingpod.Handle) (framework.Plugin, error) {
	return &NodeNumber{wt: h}, nil
}
