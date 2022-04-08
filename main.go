package main

import (
	"context"
	"strconv"
	"time"

	apiserver "github.com/shintard/minikube-scheduler/api_server"
	"github.com/shintard/minikube-scheduler/config"
	pvcontroller "github.com/shintard/minikube-scheduler/pv_controller"
	"github.com/shintard/minikube-scheduler/scheduler"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func main() {
	if err := start(); err != nil {
		klog.Fatalf("failed with error on running scheduler: %s", err.Error())
	}

}

func start() error {
	cfg, err := config.NewConfig()
	if err != nil {
		return err
	}

	restClientCfg, apiShutDown, err := apiserver.StartAPIServer(cfg.EtcdURL)
	if err != nil {
		return err
	}
	defer apiShutDown()

	client := clientset.NewForConfigOrDie(restClientCfg)

	pvShutdown, err := pvcontroller.StartPersistentVolumeController(client)
	if err != nil {
		return err
	}
	defer pvShutdown()

	sched := scheduler.NewSchedulerService(client, restClientCfg)

	dsc, err := scheduler.DefaultSchedulerConfig()
	if err != nil {
		return err
	}

	if err := sched.StartScheduler(dsc); err != nil {
		return err
	}
	defer sched.ShutdownScheduler()

	err = scenario(client)
	if err != nil {
		return err
	}

	return nil
}

func scenario(client clientset.Interface) error {
	ctx := context.Background()

	// create node0 ~ node9
	for i := 0; i < 10; i++ {
		suffix := strconv.Itoa(i)
		_, err := client.CoreV1().Nodes().Create(ctx, &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node" + suffix,
			},
		}, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}

	klog.Info("scenario: all nodes created")

	_, err := client.CoreV1().Pods("default").Create(ctx, &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod1"},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "container1",
					Image: "k8s.gcr.io/pause:3.5",
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	klog.Infof("scenario: pod1 created")

	time.Sleep(4 * time.Second)

	pod, err := client.CoreV1().Pods("default").Get(ctx, "pod1", metav1.GetOptions{})
	if err != nil {
		return err
	}

	klog.Info("scenario: pod1 is bound to " + pod.Spec.NodeName)

	return nil
}
