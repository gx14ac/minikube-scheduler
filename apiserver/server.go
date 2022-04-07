package apiserver

import (
	"net"
	"net/http"

	"github.com/google/uuid"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	authenticatorunion "k8s.io/apiserver/pkg/authentication/request/union"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/options"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/client-go/pkg/version"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controlplane"
	con "k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/pkg/kubeapiserver"
	kubeletclient "k8s.io/kubernetes/pkg/kubelet/client"
)

type APIServerHolder struct {
	SetupCh chan struct{} 
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
		IP: net.IPv4(127, 0, 0, 1),
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
	if len(kubeVersion.Major) == 0 {
		kubeVersion.Major = "22"
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
			StorageFactory: storageFactory,
			KubeletClientConfig: kubeletclient.KubeletClientConfig{Port: 10250},
			APIServerServicePort: 443,
			MasterCount: 1,
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
		Burst: 100,
		ContentConfig: restclient.ContentConfig{NegotiatedSerializer: legacyscheme.Codecs},
		Host: serverURL,
		BearerToken: privilegedLoopbackToken,
	}

	// set the user information associated with the Token
	tokens := make(map[string]*user.DefaultInfo)
	tokens[privilegedLoopbackToken] = &user.DefaultInfo{
		Name: user.APIServerUser,
		UID: uuid.New().String(),
		Groups: []string{user.SystemPrivilegedGroup},
	}

	// set token authenticator(認証局)
	// 誰がRequestを送ってきたかを認証
	tokenAuthenticator := authenticatorfactory.NewFromTokens(tokens, cfg.GenericConfig.Authentication.APIAudiences)
	cfg.GenericConfig.Authentication.Authenticator = authenticatorunion.New(tokenAuthenticator, authenticator.RequestFunc(alwaysEmpty))

	// set token authorizer(認証者)
	// 送ってきたリクエストに対して権限があるかどうかを検証
	tokenAuthorizer := authorizerfactory.NewPrivilegedGroups(user.SystemPrivilegedGroup)
	cfg.GenericConfig.Authorization.Authorizer = tokenAuthorizer

	cfg.GenericConfig.PublicAddress = net.ParseIP("192.168.10.4")
	// genericConfig.SecureServing = &genericapiserver.SecureServingInfo{Listener: fakeListener{}}
	cfg.GenericConfig.OpenAPIConfig = 


	return cfg
}
