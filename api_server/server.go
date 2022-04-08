package apiserver

import (
	"net"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/google/uuid"
	generated "github.com/shintard/minikube-scheduler/api_server/openapi"
	"golang.org/x/net/context"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	authenticatorunion "k8s.io/apiserver/pkg/authentication/request/union"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	authorizerunion "k8s.io/apiserver/pkg/authorization/union"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/options"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/version"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	con "k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/pkg/kubeapiserver"
	kubeletclient "k8s.io/kubernetes/pkg/kubelet/client"
)

func StartAPIServer(etcdURL string) (*restclient.Config, func(), error) {
	h := &APIServerHolder{SetupCh: make(chan struct{})}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// SetAPIServer関数の処理が終わるまで待機
		<-h.SetupCh
		h.PlaneInstance.GenericAPIServer.Handler.ServeHTTP(w, req)
	}))

	c := NewControlPlaneConfig(s.URL, etcdURL)
	_, _, closeFn, err := startAPIServer(c, s, h)
	if err != nil {
		return nil, nil, err
	}

	cfg := &restclient.Config{
		Host:          s.URL,
		ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}},
		QPS:           5000.0,
		Burst:         5000,
	}

	shutdownFunc := func() {
		klog.Info("destroying API server")
		closeFn()

		s.Close()
		klog.Infof("destoryed API server")
	}

	return cfg, shutdownFunc, nil
}

type APIServerHolder struct {
	SetupCh       chan struct{}
	PlaneInstance *con.Instance
}

func (h *APIServerHolder) SetAPIServer(i *con.Instance) {
	h.PlaneInstance = i
	close(h.SetupCh)
}

type fakeListener struct{}

func (fakeListener) Accept() (net.Conn, error) {
	return nil, nil
}

func (fakeListener) Close() error {
	return nil
}

func (fakeListener) Addr() net.Addr {
	return &net.TCPAddr{
		IP:   net.IPv4(127, 0, 0, 1),
		Port: 443,
	}
}

func alwaysEmpty(_ *http.Request) (*authenticator.Response, bool, error) {
	return &authenticator.Response{
		User: &user.DefaultInfo{
			Name: "",
		},
	}, true, nil
}

// openAPIに準拠した形でKubernetes API Serverを使えるように設定
func defaultOpenAPIConfig() *openapicommon.Config {
	openAPIConfig := genericapiserver.DefaultOpenAPIConfig(generated.GetOpenAPIDefinitions, openapi.NewDefinitionNamer(legacyscheme.Scheme))
	openAPIConfig.Info = &spec.Info{
		InfoProps: spec.InfoProps{
			Title:   "Kubernetes",
			Version: "unversioned",
		},
	}
	openAPIConfig.DefaultResponse = &spec.Response{
		ResponseProps: spec.ResponseProps{
			Description: "Default Response",
		},
	}
	openAPIConfig.GetDefinitions = generated.GetOpenAPIDefinitions

	return openAPIConfig
}

// Control Planeの初期化処理
// etcd, API Serverの初期設定を行い、Control Planeに設定する
func NewControlPlaneConfig(serverURL, etcdURL string) *con.Config {
	// configure etcd
	etcdOptions := options.NewEtcdOptions(storagebackend.NewDefaultConfig(uuid.New().String(), nil))
	etcdOptions.StorageConfig.Transport.ServerList = append(etcdOptions.StorageConfig.Transport.ServerList, etcdURL)

	// StorageFactoryはAPI Serverのデータをetcdなどのバックエンドに保存するために使用される
	storageConfig := kubeapiserver.NewStorageFactoryConfig()
	storageConfig.APIResourceConfig = serverstorage.NewResourceConfig()

	completeStorageConfig, err := storageConfig.Complete(etcdOptions)
	if err != nil {
		panic(err)
	}

	// 作成が完了したStorageからStorageを取り出す
	storageFactory, err := completeStorageConfig.New()
	if err != nil {
		panic(err)
	}

	// api serverのバージョンニングを調整する
	genericConfig := genericapiserver.NewConfig(legacyscheme.Codecs)
	kubeVersion := version.Get()
	if len(kubeVersion.Major) == 0 {
		kubeVersion.Major = "1"
	}
	if len(kubeVersion.Minor) == 0 {
		kubeVersion.Minor = "22"
	}
	genericConfig.Version = &kubeVersion

	// apiserverをlocalhostで登録
	genericConfig.SecureServing = &genericapiserver.SecureServingInfo{Listener: fakeListener{}}

	// etcdにStorageFactoryと疎通を行うAPI Serverを適用する
	err = etcdOptions.ApplyWithStorageFactoryTo(storageFactory, genericConfig)
	if err != nil {
		panic(err)
	}

	// create client config
	cfg := &controlplane.Config{
		GenericConfig: genericConfig,
		ExtraConfig: controlplane.ExtraConfig{
			APIResourceConfigSource: controlplane.DefaultAPIResourceConfigSource(),
			StorageFactory:          storageFactory,
			KubeletClientConfig:     kubeletclient.KubeletClientConfig{Port: 10250},
			APIServerServicePort:    443,
			MasterCount:             1,
		},
	}

	// set loopback client
	privilegedLoopbackToken := uuid.New().String()
	cfg.GenericConfig.LoopbackClientConfig = &restclient.Config{
		// cotntrol planeがapi serverと通信する1秒当たりのノードのクエリの数
		QPS: 50,
		// cotntrol planeがapi serverと通信するバーストレート
		// https://kubernetes.io/docs/tasks/configure-pod-container/assign-cpu-resource/#motivation-for-cpu-requests-and-limits
		// DefaultBurstは10
		Burst:         100,
		ContentConfig: restclient.ContentConfig{NegotiatedSerializer: legacyscheme.Codecs},
		Host:          serverURL,
		BearerToken:   privilegedLoopbackToken,
	}

	// set the user information associated with the Token
	tokens := make(map[string]*user.DefaultInfo)
	tokens[privilegedLoopbackToken] = &user.DefaultInfo{
		Name:   user.APIServerUser,
		UID:    uuid.New().String(),
		Groups: []string{user.SystemPrivilegedGroup},
	}

	// set token authenticator(認証局)
	// 誰がRequestを送ってきたかを認証
	tokenAuthenticator := authenticatorfactory.NewFromTokens(tokens, cfg.GenericConfig.Authentication.APIAudiences)
	cfg.GenericConfig.Authentication.Authenticator = authenticatorunion.New(tokenAuthenticator, authenticator.RequestFunc(alwaysEmpty))

	// set token authorizer(認証者)
	// 送ってきたリクエストに対して権限があるかどうかを検証
	tokenAuthorizer := authorizerfactory.NewPrivilegedGroups(user.SystemPrivilegedGroup)
	cfg.GenericConfig.Authorization.Authorizer = authorizerunion.New(tokenAuthorizer, authorizerfactory.NewAlwaysAllowAuthorizer())

	cfg.GenericConfig.PublicAddress = net.ParseIP("192.168.10.4")
	genericConfig.SecureServing = &genericapiserver.SecureServingInfo{Listener: fakeListener{}}
	cfg.GenericConfig.OpenAPIConfig = defaultOpenAPIConfig()

	return cfg
}

func startAPIServer(
	controlPlaneConfig *controlplane.Config,
	s *httptest.Server,
	apiServerReceiver *APIServerHolder,
) (*controlplane.Instance, *httptest.Server, func(), error) {
	var i *controlplane.Instance

	stopCh := make(chan struct{})
	closeFn := func() {
		if i != nil {
			if err := i.GenericAPIServer.RunPreShutdownHooks(); err != nil {
				klog.Errorf("failed to run pre-shutdown hooks for api server: %s", err.Error())
			}
		}
		close(stopCh)
		s.Close()
	}

	// kubernetes apiを叩くためにNewControlPlaneConfigで設定した、LoopbackClientConfigを使ってクライアントを取得
	clientset, err := clientset.NewForConfig(controlPlaneConfig.GenericConfig.LoopbackClientConfig)
	if err != nil {
		klog.Errorf("failed to create new clientset with 'LoopBackClientConfig', because: %s", err.Error())
		closeFn()
		return nil, nil, nil, err
	}

	// informersはオブジェクトを監視するらしい。オブジェクトを監視するためのアクセス可能なClientSetとタイムアウトの時間を設定している
	// informersはObjectの監視を行い、in-memory-cacheにデータを格納するコンポーネントである。裏側ではetcdに保存されている気がする
	controlPlaneConfig.ExtraConfig.VersionedInformers = informers.NewSharedInformerFactory(clientset, controlPlaneConfig.GenericConfig.LoopbackClientConfig.Timeout)

	// flow controlという機能を使って、オブジェクトの監視をするように設定する
	// https://kubernetes.io/docs/concepts/cluster-administration/flow-control/
	controlPlaneConfig.GenericConfig.FlowControl = utilflowcontrol.New(
		controlPlaneConfig.ExtraConfig.VersionedInformers,
		clientset.FlowcontrolV1beta1(),
		controlPlaneConfig.GenericConfig.MaxRequestsInFlight+controlPlaneConfig.GenericConfig.MaxMutatingRequestsInFlight,
		controlPlaneConfig.GenericConfig.RequestTimeout/4,
	)
	controlPlaneConfig.ExtraConfig.ServiceIPRange = net.IPNet{IP: net.ParseIP("10.0.0.0"), Mask: net.CIDRMask(24, 32)}

	// 設定したControl Planeからマスターコントロールプレーンのインスタンスを返す
	i, err = controlPlaneConfig.Complete().New(genericapiserver.NewEmptyDelegate())
	if err != nil {
		klog.Errorf("error in bringing up the apiserver: %s", err.Error())
	}
	apiServerReceiver.SetAPIServer(i)

	// API Serverのインストール処理を行う
	// hardwayでいうこの辺の処理をやってくれているのかな
	// https://github.com/kelseyhightower/kubernetes-the-hard-way/blob/master/docs/04-certificate-authority.md#the-kubernetes-api-server-certificate
	i.GenericAPIServer.PrepareRun()

	// API Serverの実行
	// Kubernetesが扱うコンテナのライフサイクルのなかでも似たようなものがあった
	i.GenericAPIServer.RunPostStartHooks(stopCh)

	cfg := *controlPlaneConfig.GenericConfig.LoopbackClientConfig
	// default versionを指定
	cfg.ContentConfig.GroupVersion = &schema.GroupVersion{}

	// RESTClientForは今まで設定してきたcontrol planeの設定を満たす、RESTClientを返す
	priviledgeClient, err := restclient.RESTClientFor(&cfg)
	if err != nil {
		closeFn()
		return nil, nil, nil, err
	}

	// 返却されたクライアントを通じて、Healthチェックを行う
	var lastHealthContent []byte
	err = wait.PollImmediate(100*time.Millisecond, 30*time.Second, func() (bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		result := priviledgeClient.Get().AbsPath("/healthz").Do(ctx)
		status := 0
		result.StatusCode(&status)
		if status == 200 {
			return true, nil
		}

		lastHealthContent, _ = result.Raw()
		return false, nil
	})
	if err != nil {
		close(stopCh)
		klog.Errorf("last health content: %q", string(lastHealthContent))
		return nil, nil, nil, err
	}

	return i, s, closeFn, nil
}
