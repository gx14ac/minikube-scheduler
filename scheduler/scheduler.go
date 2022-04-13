package scheduler

import (
	"context"
	"fmt"

	"github.com/shintard/minikube-scheduler/custom_scheduler"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	v1beta2config "k8s.io/kube-scheduler/config/v1beta2"
	"k8s.io/kubernetes/pkg/scheduler"
)

type SchedulerService struct {
	shutdownFn func()

	clientSet        clientset.Interface
	restClientConfig *restclient.Config
	currentSchedCfg  *v1beta2config.KubeSchedulerConfiguration
}

func NewSchedulerService(client clientset.Interface, restClientCfg *restclient.Config) *SchedulerService {
	return &SchedulerService{
		clientSet:        client,
		restClientConfig: restClientCfg,
	}
}

func (s *SchedulerService) StartScheduler(versionedCfg *v1beta2config.KubeSchedulerConfiguration) error {
	clientSet := s.clientSet
	ctx, cancel := context.WithCancel(context.Background())

	// Podを監視するinformerを取得
	informerFactory := scheduler.NewInformerFactory(clientSet, 0)

	// kubernetes clientのイベントをブロードキャストする初期化処理
	evtBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{
		Interface: clientSet.EventsV1(),
	})
	evtBroadcaster.StartRecordingToSink(ctx.Done())

	s.currentSchedCfg = versionedCfg.DeepCopy()

	// カスタムスケジューラーの初期化設定
	sched, err := custom_scheduler.NewScheduler(
		clientSet,
		informerFactory,
	)
	if err != nil {
		cancel()
		return fmt.Errorf("create sched: %w", err)
	}

	// 監視開始
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	// スケジューリングを実行
	go sched.Run(ctx)

	s.shutdownFn = cancel

	return nil
}

func (s *SchedulerService) ShutdownScheduler() {
	if s.shutdownFn != nil {
		klog.Info("shutdown scheduler...")
		s.shutdownFn()
	}
}

func (s *SchedulerService) RestartScheduler(cfg *v1beta2config.KubeSchedulerConfiguration) error {
	s.ShutdownScheduler()

	if err := s.StartScheduler(cfg); err != nil {
		return err
	}

	return nil
}
