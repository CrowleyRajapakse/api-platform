/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HubChannel is a single subscribable topic exposed by the WebSub hub.
type HubChannel struct {
	// Name is the channel name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Policies lists channel-scoped policies (e.g. rbac).
	// +optional
	Policies []Policy `json:"policies,omitempty"`
}

// WebSubHub configures the WebSub hub (subscriber management & fan-out).
type WebSubHub struct {
	// Channels lists topics available for subscription.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Channels []HubChannel `json:"channels"`

	// Policies lists hub-level policies (e.g. api-key-auth).
	// +optional
	Policies []Policy `json:"policies,omitempty"`
}

// WebSubReceiver configures inbound publisher webhook handling.
type WebSubReceiver struct {
	// Policies lists policies applied to inbound webhook requests.
	// +optional
	Policies []Policy `json:"policies,omitempty"`
}

// WebSubDelivery configures outbound subscriber delivery.
type WebSubDelivery struct {
	// Policies lists policies applied to subscriber delivery requests.
	// +optional
	Policies []Policy `json:"policies,omitempty"`
}

// WebhookAPIData mirrors the management-API WebhookAPIData payload.
type WebhookAPIData struct {
	// DisplayName is a human-readable API name.
	// +kubebuilder:validation:Required
	DisplayName string `json:"displayName"`

	// Version is the API semantic version.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^v?([0-9]+)(\.[0-9]+)?(\.[0-9]+)?(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`
	Version string `json:"version"`

	// Context is the base path for all routes (must start with /).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^/[a-zA-Z0-9\-._~!$&'()*+,;=:@%/]*[^/]$`
	Context string `json:"context"`

	// Hub configures subscriber management and fan-out.
	// +kubebuilder:validation:Required
	Hub WebSubHub `json:"hub"`

	// Receiver configures inbound publisher webhook handling.
	// +optional
	Receiver *WebSubReceiver `json:"receiver,omitempty"`

	// Delivery configures outbound subscriber delivery.
	// +optional
	Delivery *WebSubDelivery `json:"delivery,omitempty"`

	// DeploymentState toggles whether the API is router-attached.
	// +optional
	// +kubebuilder:validation:Enum=deployed;undeployed
	DeploymentState *string `json:"deploymentState,omitempty"`

	// Vhosts is an optional VhostConfig for the WebSub API.
	// +optional
	Vhosts *VhostConfig `json:"vhosts,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:path=websubapis,singular=websubapi,shortName=wsapi

// WebSubApi is the Schema for the websubapis API.
type WebSubApi struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   WebhookAPIData `json:"spec,omitempty"`
	Status ResourceStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// WebSubApiList contains a list of WebSubApi.
type WebSubApiList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []WebSubApi `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WebSubApi{}, &WebSubApiList{})
}
