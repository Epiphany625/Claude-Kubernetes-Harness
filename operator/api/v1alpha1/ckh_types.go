package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// CKHSpec is the desired state. Placeholder fields -- they exist to show what a
// spec looks like and to give codegen something to chew on.
type CKHSpec struct {
	// TargetNamespace is the namespace the harness drives.
	TargetNamespace string `json:"targetNamespace"`

	// Replicas is how many harness workers to run.
	//
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Suspend pauses reconciliation without deleting the object.
	//
	// +optional
	Suspend bool `json:"suspend,omitempty"`
}

// CKHStatus is the observed state, written only by the controller.
type CKHStatus struct {
	// ObservedGeneration is the .metadata.generation the controller last acted
	// on. Comparing it to .metadata.generation tells you whether the status
	// below describes the spec you are currently looking at.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions follow the usual Kubernetes convention; "Ready" is the one to
	// look at.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CKH is one harness instance managed by this operator.
//
// object:root=true marks it as a top-level API object (so it gets DeepCopyObject,
// not just DeepCopy), subresource:status splits .status off into its own
// endpoint, and the printcolumns are what `kubectl get ckh` shows.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetNamespace`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type CKH struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CKHSpec   `json:"spec,omitempty"`
	Status CKHStatus `json:"status,omitempty"`
}

// CKHList is the list form. Every root type needs one.
//
// +kubebuilder:object:root=true
type CKHList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []CKH `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CKH{}, &CKHList{})
}
