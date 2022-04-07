package main

import (
	"github.com/shintard/minikube-scheduler/config"
	"k8s.io/klog/v2"
)

func main() {
	if err := start(); err != nil {
		klog.Fatalf("failed with error on running scheduler: %s", err.Error())
	}
	
}

func start() error {
	_, err := config.NewConfig()
	if err != nil {
		return err
	}
	return nil
}
