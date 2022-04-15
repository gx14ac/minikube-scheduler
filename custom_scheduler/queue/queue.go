package queue

// Queueを管理するパッケージです
//

import (
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type PreEnqueueCheck func(pod *v1.Pod) bool

type QueueExecuter interface {
	Add(pod *v1.Pod)
	NextPod() *v1.Pod
}

type Queue struct {
	// Schedule待ちのPodのQueue
	activeQueue []*framework.QueuedPodInfo
	// スケジュールに失敗したPodのリトライ中のPod
	podBackoffQueue []*framework.QueuedPodInfo
	// 一度スケジュールしようとして失敗したPodのQueue。Unschedulableとなった原因のPluginなども保存
	unschedulableQueue map[string]*framework.QueuedPodInfo

	clusterEventMap map[framework.ClusterEvent]sets.String

	// mu *sync.RWMutex
	mu *sync.Cond
}

func NewQueue() *Queue {
	return &Queue{
		activeQueue:        []*framework.QueuedPodInfo{},
		podBackoffQueue:    []*framework.QueuedPodInfo{},
		unschedulableQueue: map[string]*framework.QueuedPodInfo{},
		mu:                 sync.NewCond(&sync.Mutex{}),
	}
}

// Podを使って、QueuedInfoを初期化
//
func (q *Queue) newQueuedPodInfo(pod *v1.Pod, unschedulablePlugins ...string) *framework.QueuedPodInfo {
	now := time.Now()
	return &framework.QueuedPodInfo{
		PodInfo:                 framework.NewPodInfo(pod),
		Timestamp:               now,
		InitialAttemptTimestamp: now,
		UnschedulablePlugins:    sets.NewString(unschedulablePlugins...),
	}
}

// Podが持つイベントとClusterEventが正しいかどうか確認する
// ClusterEventとはシステムリソースの状態がどのように変更されるかを抽象化したもの
//
func (q *Queue) isPodMatchesEvent(podInfo *framework.QueuedPodInfo, clusterEvent framework.ClusterEvent) bool {
	if clusterEvent.IsWildCard() {
		return true
	}

	for event, nameSet := range q.clusterEventMap {
		// プラグイン側から登録されたイベントがWildCardEventであるかどうか。
		// または、2つのイベントは同一のResourceフィールドと互換性のあるActionTypeを持っているかどうか
		eventMatch := event.IsWildCard() ||
			(event.Resource == clusterEvent.Resource && event.ActionType&clusterEvent.ActionType != 0)

		if eventMatch && intersect(nameSet, podInfo.UnschedulablePlugins) {
			return true
		}
	}

	return false
}

// podをactive or backoff queueに移動
//
func (q *Queue) movePodsToActiveOrBackOffQueue(podInfoList []*framework.QueuedPodInfo, event framework.ClusterEvent) {
	for _, pInfo := range podInfoList {
		// eventがPodをスケジューリングに該当しない、もしくはUnschedulablePluginsならcontinue
		if len(pInfo.UnschedulablePlugins) != 0 && !q.isPodMatchesEvent(pInfo, event) {
			continue
		}

		// PodInfoがBackoffの時間内ならpodBackoffQueueへ
		// そうでなければactiveQueueへ
		if isPodBackingoff(pInfo) {
			q.podBackoffQueue = append(q.podBackoffQueue, pInfo)
		} else {
			q.activeQueue = append(q.activeQueue, pInfo)
		}
		delete(q.unschedulableQueue, getFullPodName(pInfo))
	}
}

// Queueを追加する
//
func (q *Queue) Add(pod *v1.Pod) {
	q.mu.L.Lock()
	defer q.mu.L.Unlock()

	podInfo := q.newQueuedPodInfo(pod)

	q.activeQueue = append(q.activeQueue, podInfo)
	q.mu.Signal()
	return
}

// Queueを取り出す
// 取り出したらそれ以降のPodをQueueに入れる
//
func (q *Queue) NextPod() *v1.Pod {
	// wait until pod is added to queue
	q.mu.L.Lock()
	for len(q.activeQueue) == 0 {
		q.mu.Wait()
	}

	p := q.activeQueue[0]
	q.activeQueue = q.activeQueue[1:]
	q.mu.L.Unlock()
	return p.Pod
}

// unschedulableQからactiveQかbackoffQにすべてのポッドを移動
//
func (q *Queue) MoveAllToActiveOrBackoffQueue(event framework.ClusterEvent) {
	q.mu.L.Lock()
	defer q.mu.L.Unlock()

	unschedulablePods := make([]*framework.QueuedPodInfo, 0, len(q.unschedulableQueue))
	for _, pInfo := range q.unschedulableQueue {
		unschedulablePods = append(unschedulablePods, pInfo)
	}
	q.movePodsToActiveOrBackOffQueue(unschedulablePods, event)

	q.mu.Signal()
}

// PodInfoをUnschedulableQueueに名前と一緒にkey-valueに追加
//
func (q *Queue) AddUnschedulable(pInfo *framework.QueuedPodInfo) error {
	q.mu.L.Lock()
	defer q.mu.L.Unlock()

	pInfo.Timestamp = time.Now()

	q.unschedulableQueue[getFullPodName(pInfo)] = pInfo

	klog.Info("queue: pod added to unschedulableQ: "+pInfo.Pod.Name+". This pod is unscheduled by ", pInfo.UnschedulablePlugins)
	return nil
}

func (q *Queue) Update(oldPod, newPod *v1.Pod) error {
	panic("not implemented")
}

func (q *Queue) Delete(pod *v1.Pod) error {
	panic("not implemented")
}

// BindされたPodが追加されるときに呼ばれる
//　条件によっては、保留中のPodをスケジュール可能なPodにする可能性もある
//
func (q *Queue) AssignedPodAdded(pod *v1.Pod) {
	panic("not implemented")
}

// BindされたPodが更新されるときに呼ばれる
//　条件によっては、保留中のPodをスケジュール可能なPodにする可能性もある
//
func (q *Queue) AssignedPodUpdated(pod *v1.Pod) {
	panic("not implemented")
}

// バックオフを完了したbackoffQのすべてのポッドをactiveQに移動
func (q *Queue) flushBackOffQueueCompleted() {
	panic("not implemented")
}

// unschedulableQueueのTimeIntervalより長くunschedulableQueueにとどまるポッドを
// backoffQueueまたはactiveQueueに移動
func (q *Queue) flushUnschedulableQueueLeftOver() {
	panic("not implemented")
}

// utils
//
func intersect(x, y sets.String) bool {
	if len(x) > len(y) {
		x, y = y, x
	}

	for v := range x {
		if y.Has(v) {
			return true
		}
	}

	return false
}

const (
	podInitialBackoffDuration = 1 * time.Second
	podMaxBackoffDuration     = 10 * time.Second
)

// Podがスケジューリングに成功した回数に応じてBackOffDurationを追加する
//
func calculateBackoffDuration(podInfo *framework.QueuedPodInfo) time.Duration {
	duration := podInitialBackoffDuration
	for i := 1; i < podInfo.Attempts; i++ {
		// durationがpodMaxBackoffDurationを超えると、podMaxBackoffDurationを返す
		if duration > podInitialBackoffDuration-duration {
			return podMaxBackoffDuration
		}
		duration += duration
	}

	return duration
}

// PodInfoがバックオフを完了した時間を返す
//
func getBackoffTime(podInfo *framework.QueuedPodInfo) time.Time {
	duration := calculateBackoffDuration(podInfo)
	backoffTime := podInfo.Timestamp.Add(duration)
	return backoffTime
}

// Podがまだバックオフ・タイマーを待っている場合に真を返す
// 真を返した場合はポッドは再試行されるべきではない
//
func isPodBackingoff(podInfo *framework.QueuedPodInfo) bool {
	backoffTime := getBackoffTime(podInfo)
	return backoffTime.After(time.Now())
}

func getFullPodName(pInfo *framework.QueuedPodInfo) string {
	return pInfo.Pod.Name + "_" + pInfo.Pod.Namespace
}
