package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ckhv1alpha1 "github.com/Epiphany625/Claude-Kubernetes-Harness/operator/api/v1alpha1"
)

// CKHReconciler drives CKH objects towards their spec.
type CKHReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// These markers are what `make manifests` turns into the operator's Role.
// Every resource the reconciler touches needs a line here, or it will work
// locally against your own kubeconfig and then 403 inside the cluster.
//
// +kubebuilder:rbac:groups=ckh.io,resources=ckhs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ckh.io,resources=ckhs/status,verbs=get;update;patch
func (r *CKHReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	var ckh ckhv1alpha1.CKH
	if err := r.Get(ctx, req.NamespacedName, &ckh); err != nil {
		// Deleted between the event and now -- owned objects get garbage
		// collected on their own, so there is nothing to undo.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if ckh.Spec.Suspend {
		l.Info("suspended, skipping")
		return ctrl.Result{}, nil
	}

	// TODO: create/update whatever the harness needs, then write .status
	// (ObservedGeneration + the Ready condition) via r.Status().Update.
	l.Info("reconciling", "target", ckh.Spec.TargetNamespace, "replicas", ckh.Spec.Replicas)

	return ctrl.Result{}, nil
}

// SetupWithManager registers the reconciler and says what it watches.
func (r *CKHReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ckhv1alpha1.CKH{}).
		Named("ckh").
		Complete(r)
}
