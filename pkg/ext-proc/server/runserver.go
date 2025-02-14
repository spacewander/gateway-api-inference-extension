package server

import (
	"context"
	"fmt"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/types"
	klog "k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/ext-proc/backend"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/ext-proc/handlers"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/ext-proc/internal/runnable"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/ext-proc/scheduling"
)

// ExtProcServerRunner provides methods to manage an external process server.
type ExtProcServerRunner struct {
	GrpcPort                         int
	TargetEndpointKey                string
	PoolName                         string
	PoolNamespace                    string
	RefreshPodsInterval              time.Duration
	RefreshMetricsInterval           time.Duration
	RefreshMetricsTimeout            time.Duration
	RefreshPrometheusMetricsInterval time.Duration
	Datastore                        *backend.K8sDatastore
}

// Default values for CLI flags in main
const (
	DefaultGrpcPort                         = 9002                             // default for --grpcPort
	DefaultTargetEndpointKey                = "x-gateway-destination-endpoint" // default for --targetEndpointKey
	DefaultPoolName                         = ""                               // required but no default
	DefaultPoolNamespace                    = "default"                        // default for --poolNamespace
	DefaultRefreshPodsInterval              = 10 * time.Second                 // default for --refreshPodsInterval
	DefaultRefreshMetricsInterval           = 50 * time.Millisecond            // default for --refreshMetricsInterval
	DefaultRefreshMetricsTimeout            = 1 * time.Second                  // default for --refreshMetricsTimeout
	DefaultRefreshPrometheusMetricsInterval = 5 * time.Second                  // default for --refreshPrometheusMetricsInterval
)

func NewDefaultExtProcServerRunner() *ExtProcServerRunner {
	return &ExtProcServerRunner{
		GrpcPort:                         DefaultGrpcPort,
		TargetEndpointKey:                DefaultTargetEndpointKey,
		PoolName:                         DefaultPoolName,
		PoolNamespace:                    DefaultPoolNamespace,
		RefreshPodsInterval:              DefaultRefreshPodsInterval,
		RefreshMetricsInterval:           DefaultRefreshMetricsInterval,
		RefreshMetricsTimeout:            DefaultRefreshMetricsTimeout,
		RefreshPrometheusMetricsInterval: DefaultRefreshPrometheusMetricsInterval,
		// Datastore can be assigned later.
	}
}

// SetupWithManager sets up the runner with the given manager.
func (r *ExtProcServerRunner) SetupWithManager(mgr ctrl.Manager) error {
	// Create the controllers and register them with the manager
	if err := (&backend.InferencePoolReconciler{
		Datastore: r.Datastore,
		Scheme:    mgr.GetScheme(),
		Client:    mgr.GetClient(),
		PoolNamespacedName: types.NamespacedName{
			Name:      r.PoolName,
			Namespace: r.PoolNamespace,
		},
		Record: mgr.GetEventRecorderFor("InferencePool"),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed setting up InferencePoolReconciler: %w", err)
	}

	if err := (&backend.InferenceModelReconciler{
		Datastore: r.Datastore,
		Scheme:    mgr.GetScheme(),
		Client:    mgr.GetClient(),
		PoolNamespacedName: types.NamespacedName{
			Name:      r.PoolName,
			Namespace: r.PoolNamespace,
		},
		Record: mgr.GetEventRecorderFor("InferenceModel"),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed setting up InferenceModelReconciler: %w", err)
	}

	if err := (&backend.PodReconciler{
		Datastore: r.Datastore,
		Scheme:    mgr.GetScheme(),
		Client:    mgr.GetClient(),
		Record:    mgr.GetEventRecorderFor("pod"),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("failed setting up EndpointSliceReconciler: %v", err)
	}
	return nil
}

// AsRunnable returns a Runnable that can be used to start the ext-proc gRPC server.
// The runnable implements LeaderElectionRunnable with leader election disabled.
func (r *ExtProcServerRunner) AsRunnable(
	podDatastore *backend.K8sDatastore,
	podMetricsClient backend.PodMetricsClient,
) manager.Runnable {
	return runnable.NoLeaderElection(manager.RunnableFunc(func(ctx context.Context) error {
		// Initialize backend provider
		pp := backend.NewProvider(podMetricsClient, podDatastore)
		if err := pp.Init(r.RefreshPodsInterval, r.RefreshMetricsInterval, r.RefreshMetricsTimeout, r.RefreshPrometheusMetricsInterval); err != nil {
			klog.ErrorS(err, "Failed to initialize backend provider")
			return err
		}

		// Init the server.
		srv := grpc.NewServer()
		extProcPb.RegisterExternalProcessorServer(
			srv,
			handlers.NewServer(pp, scheduling.NewScheduler(pp), r.TargetEndpointKey, r.Datastore),
		)

		// Forward to the gRPC runnable.
		return runnable.GRPCServer("ext-proc", srv, r.GrpcPort).Start(ctx)
	}))
}
