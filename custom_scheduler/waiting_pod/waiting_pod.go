package waitingpod

import (
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type Handle interface {
	GetWaitingPod(uid types.UID) *WaitingPod
}

// Permitプラグインで待機されているPodの状態をもつ構造体
//
type WaitingPod struct {
	pod            *v1.Pod
	pendingPlugins map[string]*time.Timer
	status         chan *framework.Status

	mu sync.RWMutex
}

func NewWaitingPod(pod *v1.Pod, pluginsMaxWaitTime map[string]time.Duration) *WaitingPod {
	wp := &WaitingPod{
		pod:    pod,
		status: make(chan *framework.Status, 1),
	}

	wp.pendingPlugins = make(map[string]*time.Timer, len(pluginsMaxWaitTime))

	wp.mu.Lock()
	defer wp.mu.Unlock()
	// pluginが指定された時間でRejectするように
	for k, v := range pluginsMaxWaitTime {
		plugin, waitTime := k, v
		wp.pendingPlugins[plugin] = time.AfterFunc(waitTime, func() {
			msg := fmt.Sprintf("rejected due to timeout after waiting %v at plugin %v",
				waitTime, plugin)
			wp.Reject(plugin, msg)
		})
	}

	return wp
}

func (w *WaitingPod) GetPod() *v1.Pod {
	return w.pod
}

func (w *WaitingPod) GetStatus() *framework.Status {
	return <-w.status
}

func (w *WaitingPod) GetPendingPlugins() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	plugins := make([]string, 0, len(w.pendingPlugins))
	for p := range w.pendingPlugins {
		plugins = append(plugins, p)
	}

	return plugins
}

// 待機中のPodのPluginのStatusをSuccessにする
//
func (w *WaitingPod) Allow(pluginName string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if timer, exist := w.pendingPlugins[pluginName]; exist {
		timer.Stop()
		delete(w.pendingPlugins, pluginName)
	}

	if len(w.pendingPlugins) != 0 {
		return
	}

	select {
	case w.status <- framework.NewStatus(framework.Success, ""):
	default:
	}
}

// 待機中のPodのStatusをUnschedulableなものにする
//
func (w *WaitingPod) Reject(pluginName, msg string) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, timer := range w.pendingPlugins {
		timer.Stop()
	}

	select {
	case w.status <- framework.NewStatus(framework.Unschedulable, msg).WithFailedPlugin(pluginName):
	default:
	}
}
