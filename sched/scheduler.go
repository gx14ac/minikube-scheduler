package sched

import (
	"fmt"

	"github.com/shintard/minikube-scheduler/sched/queue"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// NodeにPodを配置するスケジューラー
type Scheduler struct {
	Queue *queue.Queue

	// kubernetesのクラスタとやり取りするためのインターフェース
	client clientset.Interface
}

// ノードにアサインされていないPodをQueueに追加する
//
func (s *Scheduler) addPodToQueue(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		fmt.Println("Not a Pod")
		return
	}
	s.Queue.Add(pod)
}

func isAssignPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

// NewSchedulerで呼び出される
func addEventHandlers(
	sched *Scheduler,
	informerFactory informers.SharedInformerFactory,
) {
	informerFactory.Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					// ノードにアサインされていないPodにtrueを返す
					//
					return !isAssignPod(t)
				default:
					return false
				}
			},
			// FilterFuncで返ってくるtrueなPodに対して実行される
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc: sched.addPodToQueue,
			},
		},
	)
}

func NewScheduler() *Scheduler {
	return &Scheduler{}
}
