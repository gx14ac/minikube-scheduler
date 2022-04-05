package queue

// Queueを管理するパッケージです
//

import (
	"sync"

	v1 "k8s.io/api/core/v1"
)

type QueueExecuter interface {
	Add(pod *v1.Pod)
	NextPod() *v1.Pod
}

type Queue struct {
	// Podを入れておくためのQueue
	activeQueue []*v1.Pod

	mu *sync.RWMutex
}

// Queueを追加する
func (q *Queue) Add(pod *v1.Pod) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.activeQueue = append(q.activeQueue, pod)
}

// Queueを取り出す
// 取り出したらそれ以降のPodをQueueに入れる
func (q *Queue) NextPod() *v1.Pod {
	// wait
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.activeQueue) == 0 {
	}

	p := q.activeQueue[0]
	q.activeQueue = q.activeQueue[1:]
	return p
}
