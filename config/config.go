package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port int
	EtcdURL string
	FrontendURL string
}

var (
	port int
	etcdURL string
	frontendURL string
)

func init() {
	// port
	//
	p := os.Getenv("PORT")
	if p == "" {
		panic("environment variable setting for $PORT is always required")
	}

	portString, err := strconv.Atoi(p)
	if err != nil {
		panic("failed to change port to a string cast")
	}

	port = portString

	// etcd url
	//
	eurl := os.Getenv("KUBE_SCHEDULER_SIMULATOR_ETCD_URL")
	if eurl == "" {
		panic("environment variable setting for $KUBE_SCHEDULER_SIMULATOR_ETCD_URL is always required")
	}

	etcdURL = eurl

	// frontend url
	//
	furl := os.Getenv("FRONTEND_URL")
	if furl == "" {
		panic("environment variable setting for $FRONTEND_URL is always required")
	}
	
	frontendURL = furl
}


func NewConfig() (*Config, error) {
	return &Config{
		Port: port,
		EtcdURL: etcdURL,
		FrontendURL: frontendURL,
	}, nil
}

