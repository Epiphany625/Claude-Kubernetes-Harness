// Package v1alpha1 holds the ckh.io/v1alpha1 API types.
//
// The two markers below are what make codegen work on this package:
// object:generate=true tells controller-gen to write DeepCopy methods for
// every type here, and groupName is the API group the CRDs land under.
//
// +kubebuilder:object:generate=true
// +groupName=ckh.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group/version this package serves.
	GroupVersion = schema.GroupVersion{Group: "ckh.io", Version: "v1alpha1"}

	// SchemeBuilder collects the types registered by this package.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds those types to a runtime.Scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
