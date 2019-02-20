// Copyright 2017 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// THIS FILE IS AUTOMATICALLY GENERATED.

package adapter_template_mesos

import (
	"context"
	"net"

	"istio.io/istio/mixer/pkg/adapter"
)

// The `mesos` template holds data that controls the production of mesos-specific
// attributes.
//
// Example config:
//
// ```yaml
// apiVersion: "config.istio.io/v1alpha2"
// kind: mesos
// metadata:
//   name: attributes
//   namespace: istio-system
// spec:
//   # Pass the required attribute data to the adapter
//   source_uid: source.uid | ""
//   source_ip: source.ip | ip("0.0.0.0") # default to unspecified ip addr
//   destination_uid: destination.uid | ""
//   destination_ip: destination.ip | ip("0.0.0.0") # default to unspecified ip addr
//   attribute_bindings:
//     # Fill the new attributes from the adapter produced output.
//     # $out refers to an instance of OutputTemplate message
//     source.ip: $out.source_pod_ip
//     source.labels: $out.source_labels
//     source.namespace: $out.source_namespace
//     source.service: $out.source_service
//     source.serviceAccount: $out.source_service_account_name
//     destination.ip: $out.destination_pod_ip
//     destination.labels: $out.destination_labels
//     destination.namespace: $out.destination_mamespace
//     destination.service: $out.destination_service
//     destination.serviceAccount: $out.destination_service_account_name
// ```

// Fully qualified name of the template
const TemplateName = "mesos"

// Instance is constructed by Mixer for the 'mesos' template.
//
// The `mesos` template represents data used to generate mesos-derived attributes.
//
// The values provided controls the manner in which the mesosenv adapter discovers and
// generates values related to pod information.
// Next ID: 8
type Instance struct {
	// Name of the instance as specified in configuration.
	Name string

	// Source pod's uid. Must be of the form: "mesos://pod.namespace"
	SourceUid string

	// Source pod's ip.
	SourceIp net.IP

	// Destination pod's uid. Must be of the form: "mesos://pod.namespace"
	DestinationUid string

	// Destination pod's ip.
	DestinationIp net.IP

	// Destination container's port number.
	DestinationPort int64
}

// Output struct is returned by the attribute producing adapters that handle this template.
//
// OutputTemplate refers to the output from the adapter. It is used inside the attribute_binding section of the config
// to assign values to the generated attributes using the `$out.<field name of the OutputTemplate>` syntax.
// Next ID: 33
type Output struct {
	fieldsSet map[string]bool

	// Refers to the source.uid for a pod. This is for TCP use cases where the attribute is not present.
	// attribute_bindings can refer to this field using $out.source_pod_uid
	SourcePodUid string

	// Refers to source pod ip address. attribute_bindings can refer to this field using $out.source_pod_ip
	SourcePodIp net.IP

	// Refers to source pod name. attribute_bindings can refer to this field using $out.source_pod_name
	SourcePodName string

	// Refers to source pod labels. attribute_bindings can refer to this field using $out.source_labels
	SourceLabels map[string]string

	// Refers to source pod namespace. attribute_bindings can refer to this field using $out.source_namespace
	SourceNamespace string

	// Refers to source pod service account name. attribute_bindings can refer to this field using $out.source_service_account_name
	SourceServiceAccountName string

	// Refers to source pod host ip address. attribute_bindings can refer to this field using $out.source_host_ip
	SourceHostIp net.IP

	// Refers to the Istio workload identifier for the source pod. Attribute_bindings can refer to this field using $out.source_workload_uid
	SourceWorkloadUid string

	// Refers to the Istio workload name for the source pod. Attribute_bindings can refer to this field using $out.source_workload_name
	SourceWorkloadName string

	// Refers to the Istio workload namespace for the source pod. Attribute_bindings can refer to this field using $out.source_workload_namespace
	SourceWorkloadNamespace string

	// Refers to the (controlling) owner of the source pod. Attribute_bindings can refer to this field using $out.source_owner
	SourceOwner string

	// Refers to the destination.uid for a pod. This is for TCP use cases where the attribute is not present.
	// attribute_bindings can refer to this field using $out.destination_pod_uid
	DestinationPodUid string

	// Refers to destination pod ip address. attribute_bindings can refer to this field using $out.destination_pod_ip
	DestinationPodIp net.IP

	// Refers to destination pod name. attribute_bindings can refer to this field using $out.destination_pod_name
	DestinationPodName string

	// Refers to destination container name. attribute_bindings can refer to this field using $out.destination_container_name
	DestinationContainerName string

	// Refers to destination pod labels. attribute_bindings can refer to this field using $out.destination_labels
	DestinationLabels map[string]string

	// Refers to destination pod namespace. attribute_bindings can refer to this field using $out.destination_namespace
	DestinationNamespace string

	// Refers to destination pod service account name. attribute_bindings can refer to this field using $out.destination_service_account_name
	DestinationServiceAccountName string

	// Refers to destination pod host ip address. attribute_bindings can refer to this field using $out.destination_host_ip
	DestinationHostIp net.IP

	// Refers to the (controlling) owner of the destination pod. Attribute_bindings can refer to this field using $out.destination_owner
	DestinationOwner string

	// Refers to the Istio workload identifier for the destination pod. Attribute_bindings can refer to this field using $out.destination_workload_uid
	DestinationWorkloadUid string

	// Refers to the Istio workload name for the destination pod. Attribute_bindings can refer to this field using $out.destination_workload_name
	DestinationWorkloadName string

	// Refers to the Istio workload name for the destination pod. Attribute_bindings can refer to this field using $out.destination_workload_namespace
	DestinationWorkloadNamespace string
}

func NewOutput() *Output {
	return &Output{fieldsSet: make(map[string]bool)}
}

func (o *Output) SetSourcePodUid(val string) {
	o.fieldsSet["source_pod_uid"] = true
	o.SourcePodUid = val
}

func (o *Output) SetSourcePodIp(val net.IP) {
	o.fieldsSet["source_pod_ip"] = true
	o.SourcePodIp = val
}

func (o *Output) SetSourcePodName(val string) {
	o.fieldsSet["source_pod_name"] = true
	o.SourcePodName = val
}

func (o *Output) SetSourceLabels(val map[string]string) {
	o.fieldsSet["source_labels"] = true
	o.SourceLabels = val
}

func (o *Output) SetSourceNamespace(val string) {
	o.fieldsSet["source_namespace"] = true
	o.SourceNamespace = val
}

func (o *Output) SetSourceServiceAccountName(val string) {
	o.fieldsSet["source_service_account_name"] = true
	o.SourceServiceAccountName = val
}

func (o *Output) SetSourceHostIp(val net.IP) {
	o.fieldsSet["source_host_ip"] = true
	o.SourceHostIp = val
}

func (o *Output) SetSourceWorkloadUid(val string) {
	o.fieldsSet["source_workload_uid"] = true
	o.SourceWorkloadUid = val
}

func (o *Output) SetSourceWorkloadName(val string) {
	o.fieldsSet["source_workload_name"] = true
	o.SourceWorkloadName = val
}

func (o *Output) SetSourceWorkloadNamespace(val string) {
	o.fieldsSet["source_workload_namespace"] = true
	o.SourceWorkloadNamespace = val
}

func (o *Output) SetSourceOwner(val string) {
	o.fieldsSet["source_owner"] = true
	o.SourceOwner = val
}

func (o *Output) SetDestinationPodUid(val string) {
	o.fieldsSet["destination_pod_uid"] = true
	o.DestinationPodUid = val
}

func (o *Output) SetDestinationPodIp(val net.IP) {
	o.fieldsSet["destination_pod_ip"] = true
	o.DestinationPodIp = val
}

func (o *Output) SetDestinationPodName(val string) {
	o.fieldsSet["destination_pod_name"] = true
	o.DestinationPodName = val
}

func (o *Output) SetDestinationContainerName(val string) {
	o.fieldsSet["destination_container_name"] = true
	o.DestinationContainerName = val
}

func (o *Output) SetDestinationLabels(val map[string]string) {
	o.fieldsSet["destination_labels"] = true
	o.DestinationLabels = val
}

func (o *Output) SetDestinationNamespace(val string) {
	o.fieldsSet["destination_namespace"] = true
	o.DestinationNamespace = val
}

func (o *Output) SetDestinationServiceAccountName(val string) {
	o.fieldsSet["destination_service_account_name"] = true
	o.DestinationServiceAccountName = val
}

func (o *Output) SetDestinationHostIp(val net.IP) {
	o.fieldsSet["destination_host_ip"] = true
	o.DestinationHostIp = val
}

func (o *Output) SetDestinationOwner(val string) {
	o.fieldsSet["destination_owner"] = true
	o.DestinationOwner = val
}

func (o *Output) SetDestinationWorkloadUid(val string) {
	o.fieldsSet["destination_workload_uid"] = true
	o.DestinationWorkloadUid = val
}

func (o *Output) SetDestinationWorkloadName(val string) {
	o.fieldsSet["destination_workload_name"] = true
	o.DestinationWorkloadName = val
}

func (o *Output) SetDestinationWorkloadNamespace(val string) {
	o.fieldsSet["destination_workload_namespace"] = true
	o.DestinationWorkloadNamespace = val
}

func (o *Output) WasSet(field string) bool {
	_, found := o.fieldsSet[field]
	return found
}

// HandlerBuilder must be implemented by adapters if they want to
// process data associated with the 'mesos' template.
//
// Mixer uses this interface to call into the adapter at configuration time to configure
// it with adapter-specific configuration as well as all template-specific type information.
type HandlerBuilder interface {
	adapter.HandlerBuilder
}

// Handler must be implemented by adapter code if it wants to
// process data associated with the 'mesos' template.
//
// Mixer uses this interface to call into the adapter at request time in order to dispatch
// created instances to the adapter. Adapters take the incoming instances and do what they
// need to achieve their primary function.
type Handler interface {
	adapter.Handler

	// HandleMesos is called by Mixer at request time to deliver instances to
	// to an adapter.
	GenerateMesosAttributes(context.Context, *Instance) (*Output, error)
}
