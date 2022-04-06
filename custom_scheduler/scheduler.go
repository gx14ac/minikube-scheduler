package custom_scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/shintard/minikube-scheduler/custom_scheduler/queue"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NodeにPodを配置するスケジューラー
type Scheduler struct {
	Queue queue.QueueExecuter

	// kubernetesのクラスタとやり取りするためのインターフェース
	client clientset.Interface

	// プラグインを実装するために必要なものたち
	filterPlugins   []framework.FilterPlugin
	preScopePlugins []framework.PreScorePlugin
	scopePlugins    []framework.ScorePlugin
	permitPlugins   []framework.PermitPlugin
}

func NewScheduler(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
) *Scheduler {
	sched := &Scheduler{
		Queue:  queue.NewQueue(),
		client: client,
	}

	addEventHandlers(sched, informerFactory)

	return &Scheduler{}
}

// スケジューラーはscheduleOneというメソッドを無限に実行し続けるような形で実行されている
// scheduleOneの一回の実行でPod一つのスケジュールが行われる
//
func (s *Scheduler) scheduleOne(ctx context.Context) {
	// get pod
	//
	klog.Info("scheduler: try to get pod from queue....")
	pod := s.Queue.NextPod()
	klog.Info("scheduler: start schedule(" + pod.Name + ")")

	// get nodes
	//
	nodes, err := s.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Error(err)
		return
	}
	klog.Info("schduler: successfully retrieved node")

	// select node randomly
	selectedNode := nodes.Items[rand.Intn(len(nodes.Items))]
	err = s.Bind(ctx, pod, selectedNode.Name)
	if err != nil {
		klog.Error(err)
		return
	}

	klog.Info("scheduler: Bind Pod successfully")
}

func (s *Scheduler) Bind(ctx context.Context, p *v1.Pod, nodeName string) error {
	binding := &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name, UID: p.UID},
		Target:     v1.ObjectReference{Kind: p.Kind, Namespace: nodeName},
	}

	err := s.client.CoreV1().Pods(binding.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	return nil
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

// Podがノードにアサインされているかどうか
//
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
			//
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc: sched.addPodToQueue,
			},
		},
	)
}

// func createFilterPlugins(h waitingpod.Handle) ([]framework.FilterPlugin, error) {
// 	nodeUnschedulablePlugin, err := createNodeUnschedulablePlugin()
// 	if err != nil {
// 		return nil, err
// 	}

// 	filterPlugins := []framework.FilterPlugin{
// 		nodeUnschedulablePlugin.(framework.FilterPlugin),
// 	}

// 	return filterPlugins, nil
// }

// var (
// 	nodeunschedulableplugin framework.Plugin
// 	nodenumberplugin        framework.Plugin
// )

// func createNodeUnschedulablePlugin() (framework.Plugin, error) {
// 	if nodeunschedulableplugin != nil {
// 		return nodeunschedulableplugin, nil
// 	}

// 	p, err := nodeunschedulable.New(nil, nil)
// 	nodeunschedulableplugin = p

// 	return p, err
// }
