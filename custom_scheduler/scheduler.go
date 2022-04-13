package custom_scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	node_number "github.com/shintard/minikube-scheduler/custom_scheduler/plugins/score/node_number"
	"github.com/shintard/minikube-scheduler/custom_scheduler/queue"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
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
	filterPlugins []framework.FilterPlugin
	scopePlugins  []framework.ScorePlugin
}

func NewScheduler(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
) (*Scheduler, error) {
	sched := &Scheduler{
		Queue:  queue.NewQueue(),
		client: client,
	}

	filterP, err := createFilterPlugins()
	if err != nil {
		return nil, fmt.Errorf("create filter plugins: %w", err)
	}
	sched.filterPlugins = filterP

	scoreP, err := createScorePlugins()
	if err != nil {
		return nil, fmt.Errorf("create score plugins: %w", err)
	}
	sched.scopePlugins = scoreP

	addEventHandlers(sched, informerFactory)

	return sched, nil
}

func (s *Scheduler) Run(ctx context.Context) {
	wait.UntilWithContext(ctx, s.scheduleOne, 0)
}

// スケジューラーはscheduleOneというメソッドを無限に実行し続けるような形で実行されている
// scheduleOneの一回の実行でPod一つのスケジュールが行われる
//
func (s *Scheduler) scheduleOne(ctx context.Context) {
	// get pod
	klog.Info("scheduler: try to get pod from queue....")
	pod := s.Queue.NextPod()
	klog.Info("scheduler: start schedule(" + pod.Name + ")")

	// get nodes
	nodes, err := s.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Error(err)
		return
	}
	klog.Info("scheduler: successfully retrieved node")

	//filter
	feasibleNodes, err := s.RunFilterPlugins(ctx, nil, pod, nodes.Items)
	if err != nil {
		klog.Error(err)
		return
	}
	klog.Infof("scheduler: feasible nodes ", feasibleNodes)

	// score
	score, status := s.RunScorePlugins(ctx, nil, pod, feasibleNodes)
	if !status.IsSuccess() {
		klog.Error(status.AsError().Error())
		return
	}

	klog.Info("scheduler: score results ", score)

	// select node randomly
	selectedNode := nodes.Items[rand.Intn(len(nodes.Items))]

	klog.Info("schduler: selected node " + selectedNode.Name)
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
		Target:     v1.ObjectReference{Kind: "Node", Name: nodeName},
	}

	err := s.client.CoreV1().Pods(binding.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (s *Scheduler) RunFilterPlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []v1.Node) ([]*v1.Node, error) {
	// Filterをくぐり抜けたNodes
	feasibleNodes := make([]*v1.Node, 0, len(nodes))

	// 何かしらのFilterプラグインに除外されたNodeがどのプラグインに拒否されたのかを保存しておく
	// Filterの診断結果
	// スケジューリングの不具合を診断するための内容が記録される。
	diagnosis := framework.Diagnosis{
		NodeToStatusMap:      make(framework.NodeToStatusMap),
		UnschedulablePlugins: sets.NewString(),
	}

	for _, n := range nodes {
		// nodeInfoに各Nodeをセット
		nodeInfo := framework.NewNodeInfo()
		nodeInfo.SetNode(&n)

		status := framework.NewStatus(framework.Success)
		for _, pl := range s.filterPlugins {
			// Filterプラグインの実行
			status = pl.Filter(ctx, state, pod, nodeInfo)
			// プラグインがNodeを不適格とした場合
			// Filterプラグインの不具合をdiagnosisに保存
			if !status.IsSuccess() {
				status.SetFailedPlugin(pl.Name())
				diagnosis.UnschedulablePlugins.Insert(status.FailedPlugin())
				break
			}
		}
		if status.IsSuccess() {
			feasibleNodes = append(feasibleNodes, nodeInfo.Node())
		}
	}

	if len(feasibleNodes) == 0 {
		return nil, &framework.FitError{
			Pod:       pod,
			Diagnosis: diagnosis,
		}
	}

	return feasibleNodes, nil
}

func (s *Scheduler) RunScorePlugins(
	ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	nodes []*v1.Node,
) (framework.NodeScoreList, *framework.Status) {
	scoresMap := s.createPluginToNodeScores(nodes)

	// 各NodeのPluginを全て確認し、スコアリングする
	for index, n := range nodes {
		for _, pl := range s.scopePlugins {
			score, status := pl.Score(ctx, state, pod, n.Name)
			if !status.IsSuccess() {
				return nil, status
			}
			scoresMap[pl.Name()][index] = framework.NodeScore{
				Name:  n.Name,
				Score: score,
			}
		}
	}

	// スコアリングの集計を行う
	result := make(framework.NodeScoreList, 0, len(nodes))
	for i := range nodes {
		result = append(result, framework.NodeScore{Name: nodes[i].Name, Score: 0})
		for j := range scoresMap {
			result[i].Score += scoresMap[j][i].Score
		}
	}

	return result, nil
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
	klog.Infof("add pod to queue: %s", pod.Name)
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

// 各PluginのScoreをnodesの長さで作成
func (s *Scheduler) createPluginToNodeScores(nodes []*v1.Node) framework.PluginToNodeScores {
	pluginToNodeScores := make(framework.PluginToNodeScores, len(nodes))
	for _, pl := range s.scopePlugins {
		pluginToNodeScores[pl.Name()] = make(framework.NodeScoreList, len(nodes))
	}

	return pluginToNodeScores
}

// create plugins
//
func createFilterPlugins() ([]framework.FilterPlugin, error) {
	nodeUnschedulablePlugin, err := createNodeUnschedulablePlugin()
	if err != nil {
		return nil, fmt.Errorf("create nodeUnschedulable Plugin: %w", err)
	}

	filterPlugins := []framework.FilterPlugin{
		nodeUnschedulablePlugin.(framework.FilterPlugin),
	}

	return filterPlugins, nil
}

func createScorePlugins() ([]framework.ScorePlugin, error) {
	nodeNumberPlugin, err := createNodeNumberPlugin()
	if err != nil {
		return nil, fmt.Errorf("create nodeNumber Plugin: %w", err)
	}

	scorePlugins := []framework.ScorePlugin{
		nodeNumberPlugin.(framework.ScorePlugin),
	}

	return scorePlugins, nil
}

// initialize plugins
//
var (
	nodeUnschedulablePlugin framework.Plugin
	nodeNumberPlugin        framework.Plugin
)

func createNodeUnschedulablePlugin() (framework.Plugin, error) {
	if nodeUnschedulablePlugin != nil {
		return nodeUnschedulablePlugin, nil
	}

	p, err := nodeunschedulable.New(nil, nil)
	nodeUnschedulablePlugin = p
	return p, err
}

func createNodeNumberPlugin() (framework.Plugin, error) {
	if nodeNumberPlugin != nil {
		return nodeNumberPlugin, nil
	}

	p, err := node_number.New(nil, nil)
	nodeNumberPlugin = p

	return p, err
}
