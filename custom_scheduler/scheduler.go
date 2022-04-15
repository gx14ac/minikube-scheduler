package custom_scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	node_number "github.com/shintard/minikube-scheduler/custom_scheduler/plugins/score/node_number"
	"github.com/shintard/minikube-scheduler/custom_scheduler/queue"
	waitingpod "github.com/shintard/minikube-scheduler/custom_scheduler/waiting_pod"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

	waitingPods map[types.UID]*waitingpod.WaitingPod

	// プラグインを実装するために必要なものたち
	filterPlugins   []framework.FilterPlugin
	preScorePlugins []framework.PreScorePlugin
	permitPlugins   []framework.PermitPlugin
	scopePlugins    []framework.ScorePlugin
}

func NewScheduler(
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
) (*Scheduler, error) {
	sched := &Scheduler{
		Queue:       queue.NewQueue(),
		client:      client,
		waitingPods: map[types.UID]*waitingpod.WaitingPod{},
	}

	filterP, err := createFilterPlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create filter plugins: %w", err)
	}
	sched.filterPlugins = filterP

	preScoreP, err := createPreScorePlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create pre score plugins: %w", err)
	}
	sched.preScorePlugins = preScoreP

	scoreP, err := createScorePlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create score plugins: %w", err)
	}
	sched.scopePlugins = scoreP

	permitP, err := createPermitPlugins(sched)
	if err != nil {
		return nil, fmt.Errorf("create permit plugins: %w", err)
	}
	sched.permitPlugins = permitP

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

	state := framework.NewCycleState()

	// get nodes
	nodes, err := s.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Error(err)
		s.ErrorFunc(pod, err)
		return
	}
	klog.Info("scheduler: successfully retrieved node")

	//filter
	feasibleNodes, status := s.RunFilterPlugins(ctx, state, pod, nodes.Items)
	if !status.IsSuccess() {
		klog.Error(err)
		s.ErrorFunc(pod, err)
		return
	}
	klog.Infof("scheduler: feasible nodes ", feasibleNodes)

	// pre score
	status = s.RunPreScorePlugins(ctx, state, pod, feasibleNodes)
	if !status.IsSuccess() {
		klog.Error(status.AsError())
		s.ErrorFunc(pod, err)
		return
	}
	klog.Infof("scheduler: run pre score plugins successfully ")

	// score
	score, status := s.RunScorePlugins(ctx, state, pod, feasibleNodes)
	if !status.IsSuccess() {
		klog.Error(status.AsError().Error())
		s.ErrorFunc(pod, err)
		return
	}

	klog.Info("scheduler: score results ", score)

	nodeName, err := s.selectHost(score)
	if err != nil {
		klog.Error(err)
		s.ErrorFunc(pod, err)
		return
	}

	klog.Info("scheduler: pod " + pod.Name + " will be bound to node " + nodeName)

	status = s.RunPermitPlugins(ctx, state, pod, nodeName)
	if !status.IsWait() && !status.IsSuccess() {
		klog.Error(status.AsError())
		s.ErrorFunc(pod, err)
		return
	}

	go func() {
		ctx := ctx

		status := s.WaitOnPermit(ctx, pod)
		if !status.IsSuccess() {
			klog.Error(status.AsError())
			s.ErrorFunc(pod, err)
			return
		}

		if err := s.Bind(ctx, pod, nodeName); err != nil {
			klog.Error(err)
			s.ErrorFunc(pod, err)
			return
		}

		klog.Info("scheduler: Bind Pod Successfully")
	}()
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

func (s *Scheduler) RunFilterPlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []v1.Node) ([]*v1.Node, *framework.Status) {
	// Filterをくぐり抜けたNodes
	feasibleNodes := make([]*v1.Node, 0, len(nodes))

	for _, n := range nodes {
		// nodeInfoに各Nodeをセット
		n := n
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
				break
			}
		}
		if status.IsSuccess() {
			feasibleNodes = append(feasibleNodes, nodeInfo.Node())
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

func (s *Scheduler) RunPreScorePlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) *framework.Status {
	for _, pl := range s.preScorePlugins {
		status := pl.PreScore(ctx, state, pod, nodes)
		if !status.IsSuccess() {
			return status
		}
	}

	return nil
}

// Bind Cycleの実行を中止、遅延を行う
func (s *Scheduler) RunPermitPlugins(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (status *framework.Status) {
	pluginsWaitTime := make(map[string]time.Duration)
	statusCode := framework.Success
	for _, pl := range s.permitPlugins {
		status, timeOut := pl.Permit(ctx, state, pod, nodeName)
		if !status.IsSuccess() {
			// reject
			if status.IsUnschedulable() {
				klog.InfoS("Pod rejected by permit plugin", "pod", klog.KObj(pod), "plugin", pl.Name(), "status", status.Message())
				status.SetFailedPlugin(pl.Name())
				return status
			}

			// wait
			if status.IsWait() {
				pluginsWaitTime[pl.Name()] = timeOut
				statusCode = framework.Wait
				continue
			}

			// other errors
			err := status.AsError()
			klog.ErrorS(err, "Failed running Permit plugin", "plugin", pl.Name(), "pod", klog.KObj(pod))
			return framework.AsStatus(fmt.Errorf("running permit plugin %q: %w", pl.Name(), err)).WithFailedPlugin(pl.Name())
		}
	}

	if statusCode == framework.Wait {
		// waitingPodの作成
		// waitingPodはwait状態のPodに対して、「どのPluginが何秒までに結果を出すのか」を保持している構造体
		waitingPod := waitingpod.NewWaitingPod(pod, pluginsWaitTime)
		// waitingPodをスケジューラーに保存 (waitOnPermitで使用する)
		s.waitingPods[pod.UID] = waitingPod
		msg := fmt.Sprintf("One or more plugins asked to wait and no plugin rejected pod %q", pod.Name)
		klog.InfoS("One or More plugins asked to wait and no plugin rejected pod", "pod", klog.KObj(pod))
		return framework.NewStatus(framework.Wait, msg)
	}

	return nil
}

func (s *Scheduler) WaitOnPermit(ctx context.Context, pod *v1.Pod) *framework.Status {
	wp := s.waitingPods[pod.UID]
	if wp == nil {
		return nil
	}
	defer delete(s.waitingPods, pod.UID)

	klog.InfoS("Pod waiting on permit", "pod", klog.KObj(pod))

	// Nodeが指定された時間にwaitingPodのStatusに状態を送るので、その状態をSignalが受け取る
	status := wp.GetStatus()

	if !status.IsSuccess() {
		if status.IsUnschedulable() {
			klog.InfoS("Pod rejected while waiting on permit", "pod", klog.KObj(pod), "status", status.Message())

			status.SetFailedPlugin(status.FailedPlugin())
			return status
		}

		err := status.AsError()
		klog.ErrorS(err, "Failed waiting on permit for pod", "pod", klog.KObj(pod))
		return framework.AsStatus(fmt.Errorf("waiting on permit for pod: %w", err)).WithFailedPlugin(status.FailedPlugin())
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
	pluginToNodeScores := make(framework.PluginToNodeScores, len(s.scopePlugins))
	for _, pl := range s.scopePlugins {
		pluginToNodeScores[pl.Name()] = make(framework.NodeScoreList, len(nodes))
	}

	return pluginToNodeScores
}

// 一番スコアの高いNodeの名前を返却する
func (s *Scheduler) selectHost(nodeScoreList framework.NodeScoreList) (string, error) {
	if len(nodeScoreList) == 0 {
		return "", fmt.Errorf("empty priorityList")
	}
	maxScore := nodeScoreList[0].Score
	selected := nodeScoreList[0].Name
	cntOfMaxScore := 1
	for _, ns := range nodeScoreList[1:] {
		if ns.Score > maxScore {
			maxScore = ns.Score
			selected = ns.Name
			cntOfMaxScore = 1
		} else if ns.Score == maxScore {
			cntOfMaxScore++
			if rand.Intn(cntOfMaxScore) == 0 {
				// Replace the candidate with probability of 1/cntOfMaxScore
				selected = ns.Name
			}
		}
	}
	return selected, nil
}

func (s *Scheduler) GetWaitingPod(uid types.UID) *waitingpod.WaitingPod {
	return s.waitingPods[uid]
}

func (s *Scheduler) ErrorFunc(pod *v1.Pod, err error) {
	podInfo := &framework.QueuedPodInfo{
		PodInfo: framework.NewPodInfo(pod),
	}

	// Filter拡張点で候補のNodeが一つも残らず、スケジュールに失敗した時
	if fitError, ok := err.(*framework.FitError); ok {
		// fitError.Diagnosis.UnschedulablePluginsには
		// スケジュール時にFilter拡張点にて一つ以上のNodeを却下した全てのプラグインが含まれている
		podInfo.UnschedulablePlugins = fitError.Diagnosis.UnschedulablePlugins
		klog.V(2).Info("Unable to schedule pod; retrying", "pod", klog.KObj(pod))
	} else {
		klog.ErrorS(err, "Error scheduling pod; retrying", "pod", klog.KObj(pod))
	}

	if err := s.Queue.AddUnschedulable(podInfo); err != nil {
		klog.ErrorS(err, "Error occurred")
	}
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

func createNodeNumberPlugin(h waitingpod.Handle) (framework.Plugin, error) {
	if nodeNumberPlugin != nil {
		return nodeNumberPlugin, nil
	}

	p, err := node_number.New(nil, h)
	nodeNumberPlugin = p

	return p, err
}

func createFilterPlugins(h waitingpod.Handle) ([]framework.FilterPlugin, error) {
	nodeUnschedulablePlugin, err := createNodeUnschedulablePlugin()
	if err != nil {
		return nil, fmt.Errorf("create nodeUnschedulable Plugin: %w", err)
	}

	filterPlugins := []framework.FilterPlugin{
		nodeUnschedulablePlugin.(framework.FilterPlugin),
	}

	return filterPlugins, nil
}

func createPreScorePlugins(h waitingpod.Handle) ([]framework.PreScorePlugin, error) {
	// NodeNumber Pluginの　PreScoreなので、どのプラグインのPreScoreを実行するか知る必要がある
	nodeNumberPlugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create nodeNumber Plugin: %w", err)
	}

	preScorePlugins := []framework.PreScorePlugin{
		nodeNumberPlugin.(framework.PreScorePlugin),
	}

	return preScorePlugins, nil
}

func createScorePlugins(h waitingpod.Handle) ([]framework.ScorePlugin, error) {
	nodeNumberPlugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create nodeNumber Plugin: %w", err)
	}

	scorePlugins := []framework.ScorePlugin{
		nodeNumberPlugin.(framework.ScorePlugin),
	}

	return scorePlugins, nil
}

func createPermitPlugins(h waitingpod.Handle) ([]framework.PermitPlugin, error) {
	nodeNumberPlugin, err := createNodeNumberPlugin(h)
	if err != nil {
		return nil, fmt.Errorf("create nodeNumber Plugin: %w", err)
	}

	permitPlugins := []framework.PermitPlugin{
		nodeNumberPlugin.(framework.PermitPlugin),
	}

	return permitPlugins, nil
}
