package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/multierr"
	klog "k8s.io/klog/v2"
)

func NewProvider(pmc PodMetricsClient, datastore *K8sDatastore) *Provider {
	p := &Provider{
		podMetrics: sync.Map{},
		pmc:        pmc,
		datastore:  datastore,
	}
	return p
}

// Provider provides backend pods and information such as metrics.
type Provider struct {
	// key: Pod, value: *PodMetrics
	podMetrics sync.Map
	pmc        PodMetricsClient
	datastore  *K8sDatastore
}

type PodMetricsClient interface {
	FetchMetrics(ctx context.Context, pod Pod, existing *PodMetrics) (*PodMetrics, error)
}

func isPodMetricsStale(pm *PodMetrics) bool {
	// TODO: make it configurable
	return time.Since(pm.UpdatedTime) > 5*time.Second
}

func (p *Provider) AllPodMetrics() []*PodMetrics {
	return p.allPodMetrics(false)
}

func (p *Provider) AllPodMetricsIncludingStale() []*PodMetrics {
	return p.allPodMetrics(true)
}

func (p *Provider) allPodMetrics(staleIncluded bool) []*PodMetrics {
	res := []*PodMetrics{}
	fn := func(k, v any) bool {
		m := v.(*PodMetrics)

		if !staleIncluded && isPodMetricsStale(m) {
			// exclude stale metrics for scheduler
			klog.V(4).Infof("Pod metrics for %s is stale, skipping", m.Pod)
			return true
		}

		res = append(res, m)
		return true
	}
	p.podMetrics.Range(fn)
	return res
}

func (p *Provider) UpdatePodMetrics(pod Pod, pm *PodMetrics) {
	pm.UpdatedTime = time.Now()
	p.podMetrics.Store(pod, pm)
}

func (p *Provider) GetPodMetrics(pod Pod) (*PodMetrics, bool) {
	val, ok := p.podMetrics.Load(pod)
	if ok {
		// For now, we don't exclude stale metrics with GET operation.
		return val.(*PodMetrics), true
	}
	return nil, false
}

func (p *Provider) Init(refreshPodsInterval, refreshMetricsInterval time.Duration) error {
	p.refreshPodsOnce()

	if err := p.refreshMetricsOnce(refreshMetricsInterval); err != nil {
		klog.Errorf("Failed to init metrics: %v", err)
	}

	klog.Infof("Initialized pods and metrics: %+v", p.AllPodMetricsIncludingStale())

	// periodically refresh pods
	go func() {
		for {
			time.Sleep(refreshPodsInterval)
			p.refreshPodsOnce()
		}
	}()

	// periodically refresh metrics
	go func() {
		time.Sleep(refreshMetricsInterval)
		for {
			start := time.Now()

			if err := p.refreshMetricsOnce(refreshMetricsInterval); err != nil {
				klog.Errorf("Failed to refresh metrics: %v", err)
			}

			now := time.Now()
			used := now.Sub(start)
			if used < refreshMetricsInterval {
				time.Sleep(refreshMetricsInterval - used)
			}
		}
	}()

	// Periodically print out the pods and metrics for DEBUGGING.
	if klog.V(2).Enabled() {
		go func() {
			for {
				time.Sleep(5 * time.Second)
				klog.Infof("===DEBUG: Current Pods and metrics: %+v", p.AllPodMetricsIncludingStale())
			}
		}()
	}

	return nil
}

// refreshPodsOnce lists pods and updates keys in the podMetrics map.
// Note this function doesn't update the PodMetrics value, it's done separately.
func (p *Provider) refreshPodsOnce() {
	// merge new pods with cached ones.
	// add new pod to the map
	addNewPods := func(k, v any) bool {
		pod := k.(Pod)
		if _, ok := p.podMetrics.Load(pod); !ok {
			new := &PodMetrics{
				Pod: pod,
				Metrics: Metrics{
					ActiveModels: make(map[string]int),
				},
			}
			p.podMetrics.Store(pod, new)
		}
		return true
	}
	// remove pods that don't exist any more.
	mergeFn := func(k, v any) bool {
		pod := k.(Pod)
		if _, ok := p.datastore.pods.Load(pod); !ok {
			p.podMetrics.Delete(pod)
		}
		return true
	}
	p.podMetrics.Range(mergeFn)
	p.datastore.pods.Range(addNewPods)
}

func (p *Provider) refreshMetricsOnce(interval time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), interval)
	defer cancel()
	start := time.Now()
	defer func() {
		d := time.Since(start)
		// TODO: add a metric instead of logging
		klog.V(4).Infof("Refreshed metrics in %v", d)
	}()
	var wg sync.WaitGroup
	errCh := make(chan error)
	processOnePod := func(key, value any) bool {
		klog.V(4).Infof("Processing pod %v and metric %v", key, value)
		pod := key.(Pod)
		existing := value.(*PodMetrics)
		wg.Add(1)
		go func() {
			defer wg.Done()
			updated, err := p.pmc.FetchMetrics(ctx, pod, existing)
			if err != nil {
				// handle timeout error as less severe error
				if errors.Is(err, context.Canceled) {
					klog.V(4).Infof("Timeout fetching metrics for pod %s", pod)
				} else {
					errCh <- fmt.Errorf("failed to fetch metrics from %s: %v", pod, err)
				}
				return
			}
			p.UpdatePodMetrics(pod, updated)
			klog.V(4).Infof("Updated metrics for pod %s: %v", pod, updated.Metrics)
		}()
		return true
	}
	p.podMetrics.Range(processOnePod)

	// Wait for metric collection for all pods to complete and close the error channel in a
	// goroutine so this is unblocking, allowing the code to proceed to the error collection code
	// below.
	// Note we couldn't use a buffered error channel with a size because the size of the podMetrics
	// sync.Map is unknown beforehand.
	go func() {
		wg.Wait()
		close(errCh)
	}()

	var errs error
	for err := range errCh {
		errs = multierr.Append(errs, err)
	}
	return errs
}
