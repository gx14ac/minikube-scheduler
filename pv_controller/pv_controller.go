package pvcontroller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/controller/volume/persistentvolume"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/hostpath"
	"k8s.io/kubernetes/pkg/volume/local"
)

// クラスター内で扱う永続的なボリュームの作成
func StartPersistentVolumeController(client clientset.Interface) (func(), error) {
	ctx, cancel := context.WithCancel(context.Background())
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	params := persistentvolume.ControllerParameters{
		KubeClient:                client,
		SyncPeriod:                1 * time.Second,
		VolumePlugins:             append(local.ProbeVolumePlugins(), hostpath.ProbeVolumePlugins(volume.VolumeConfig{})...),
		VolumeInformer:            informerFactory.Core().V1().PersistentVolumes(),
		ClaimInformer:             informerFactory.Core().V1().PersistentVolumeClaims(),
		ClassInformer:             informerFactory.Storage().V1().StorageClasses(),
		PodInformer:               informerFactory.Core().V1().Pods(),
		NodeInformer:              informerFactory.Core().V1().Nodes(),
		EnableDynamicProvisioning: true,
	}
	volumeController, err := persistentvolume.NewController(params)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to persistentvolume controller: %s", err.Error())
	}

	// volume controllerを実行
	go volumeController.Run(ctx.Done())

	// volume controllerを実行した後に監視を開始する
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	return cancel, nil
}
