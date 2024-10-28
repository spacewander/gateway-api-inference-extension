// Package scheduling implements request scheduling algorithms.
package scheduling

import (
	"fmt"
	"math/rand"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	klog "k8s.io/klog/v2"

	"ext-proc/backend"
)

const (
	// TODO(https://github.com/kubernetes-sigs/llm-instance-gateway/issues/16) Make this configurable.
	kvCacheThreshold = 0.8
	// TODO(https://github.com/kubernetes-sigs/llm-instance-gateway/issues/16) Make this configurable.
	queueThreshold = 5
)

var (
	defaultFilter = &filter{
		name:          "critical request",
		filter:        toFilterFunc(criticalRequestPredicate),
		nextOnSuccess: lowLatencyFilter,
		nextOnFailure: sheddableRequestFilter,
	}

	// lowLatencyFilter tries to minimize the latency. The heuristic is to pick a server with lower
	// cost to load an adapter and has low KV cache, which typically yields lower latency.
	lowLatencyFilter = &filter{
		name:   "least queuing",
		filter: leastQueuingFilterFunc,
		nextOnSuccessOrFailure: &filter{
			name:   "low cost LoRA",
			filter: toFilterFunc(lowLoRACostPredicate),
			nextOnSuccessOrFailure: &filter{
				name:   "least KV cache percent",
				filter: leastKVCacheFilterFunc,
			},
		},
	}

	sheddableRequestFilter = &filter{
		// When there is at least one model server that's not queuing requests, and still has KV
		// cache below a certain threshold, we consider this model server has capacity to handle
		// a sheddable request without impacting critical requests.
		name:          "has capacity for sheddable requests",
		filter:        toFilterFunc(noQueueAndLessThanKVCacheThresholdPredicate(queueThreshold, kvCacheThreshold)),
		nextOnSuccess: lowLatencyFilter,
		// If all pods are queuing or running above the KVCache threshold, we drop the sheddable
		// request to make room for critical requests.
		nextOnFailure: &filter{
			name: "drop request",
			filter: func(req *LLMRequest, pods []*backend.PodMetrics) ([]*backend.PodMetrics, error) {
				klog.Infof("Dropping request %v", req)
				return []*backend.PodMetrics{}, status.Errorf(codes.ResourceExhausted, "dropping request due to limited backend resources")
			},
		},
	}
)

func NewScheduler(pmp PodMetricsProvider) *Scheduler {
	return &Scheduler{
		podMetricsProvider: pmp,
		filter:             defaultFilter,
	}
}

type Scheduler struct {
	podMetricsProvider PodMetricsProvider
	filter             Filter
}

// PodMetricsProvider is an interface to provide set of pods in the backend and information such as
// metrics.
type PodMetricsProvider interface {
	AllPodMetrics() []*backend.PodMetrics
}

// Schedule finds the target pod based on metrics and the requested lora adapter.
func (s *Scheduler) Schedule(req *LLMRequest) (targetPod *backend.Pod, err error) {
	klog.V(3).Infof("request: %v; metrics: %+v", req, s.podMetricsProvider.AllPodMetrics())
	pods, err := s.filter.Filter(req, s.podMetricsProvider.AllPodMetrics())
	if err != nil || len(pods) == 0 {
		return nil, fmt.Errorf("failed to apply filter, resulted %v pods, this should never happen: %w", len(pods), err)
	}
	klog.V(3).Infof("Going to randomly select a pod from the candidates: %+v", pods)
	i := rand.Intn(len(pods))
	return &pods[i].Pod, nil
}