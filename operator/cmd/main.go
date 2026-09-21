// Command operator runs the CKH controller.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	ckhv1alpha1 "github.com/Epiphany625/Claude-Kubernetes-Harness/operator/api/v1alpha1"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/operator/internal/controller"
)

// scheme maps Go types to GroupVersionKinds. The client cannot read or write a
// type that is not registered here.
var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(ckhv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var leaderElect bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "Address for the metrics endpoint; 0 disables it.")
	flag.BoolVar(&leaderElect, "leader-elect", false, "Run leader election so only one replica reconciles.")
	zapOpts := zap.Options{Development: true}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	setupLog := ctrl.Log.WithName("setup")

	// GetConfigOrDie uses in-cluster credentials when there are any, and falls
	// back to ~/.kube/config -- which is what makes `make run` work.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:           scheme,
		Metrics:          metricsserver.Options{BindAddress: metricsAddr},
		LeaderElection:   leaderElect,
		LeaderElectionID: "ckh-operator.ckh.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to build manager")
		os.Exit(1)
	}

	if err := (&controller.CKHReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up controller", "controller", "CKH")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited with error")
		os.Exit(1)
	}
}
