package main

import (
	"github.com/shintard/minikube-scheduler/custom_scheduler"
	"github.com/shintard/minikube-scheduler/custom_scheduler/queue"
)

func main() {
	_ = queue.NewQueue()
	_ = custom_scheduler.Scheduler{}
}
