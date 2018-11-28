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

package mesos

import (
	"time"
	"context"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/log"
	"sync"
	"github.com/mesos/go-proto/mesos/v1/master"

	"github.com/miroswan/mesops/pkg/v1"

)

type MesosCall struct {
	Type string `json:"type"`
}

// Controller communicates with Consul and monitors for changes
type Controller struct {
	// ClusterID identifies the remote cluster in a multicluster env.
	ClusterID string

	// XDSUpdater will push EDS changes to the ADS model.
	XDSUpdater model.XDSUpdater

	stop chan struct{}

	sync.RWMutex
	// servicesMap stores hostname ==> service, it is used to reduce convertService calls.
	servicesMap map[model.Hostname]*model.Service

	client *v1.Master
}

// NewController creates a new Consul controller
func NewController(serverURL string, timeout time.Duration) (*Controller, error) {
	log.Infof("serverURL: %v, timeout: %v", serverURL, timeout)

	client, err := v1.NewMasterBuilder(serverURL).Build()
	if err != nil {
		return nil, err
	}

	return &Controller{
		client: client,
	}, nil
}

// Services list declarations of all services in the system
func (c *Controller) Services() ([]*model.Service, error) {
	log.Info("GetServices")
	return nil, nil
}

// GetService retrieves a service by host name if it exists
func (c *Controller) GetService(hostname model.Hostname) (*model.Service, error) {
	log.Infof("GetService hostname: %v", hostname)

	return nil, nil
}

// ManagementPorts retrieves set of health check ports by instance IP.
// This does not apply to Consul service registry, as Consul does not
// manage the service instances. In future, when we integrate Nomad, we
// might revisit this function.
func (c *Controller) ManagementPorts(addr string) model.PortList {
	return nil
}

// WorkloadHealthCheckInfo retrieves set of health check info by instance IP.
// This does not apply to Consul service registry, as Consul does not
// manage the service instances. In future, when we integrate Nomad, we
// might revisit this function.
func (c *Controller) WorkloadHealthCheckInfo(addr string) model.ProbeList {
	return nil
}

// InstancesByPort retrieves instances for a service that match
// any of the supplied labels. All instances match an empty tag list.
func (c *Controller) InstancesByPort(hostname model.Hostname, port int,
	labels model.LabelsCollection) ([]*model.ServiceInstance, error) {

	log.Infof("hostname: %v, port: %v, labels: %v", hostname, port, labels)
	return nil, nil
}

// returns true if an instance's port matches with any in the provided list
func portMatch(instance *model.ServiceInstance, port int) bool {
	return port == 0 || port == instance.Endpoint.ServicePort.Port
}

// GetProxyServiceInstances lists service instances co-located with a given proxy
func (c *Controller) GetProxyServiceInstances(node *model.Proxy) ([]*model.ServiceInstance, error) {
	log.Infof("node: %v", node)


	return nil, nil
}

// Run all controllers until a signal is received
func (c *Controller) Run(stop <-chan struct{}) {

	es := make(v1.EventStream, 0)
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	go func() {
		err := c.client.Subscribe(ctx, es)
		if err != nil {
			log.Fatalf("Failed to subscribe Mesos-operator API: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			break
		case e := <-es:
			eventType := e.GetType()
			switch eventType {
			case mesos_v1_master.Event_SUBSCRIBED:
				log.Infof("Event_SUBSCRIBED %v", e.GetSubscribed().GetGetState())
			case mesos_v1_master.Event_TASK_ADDED:
				log.Infof("Event_SUBSCRIBED %v", e.GetTaskAdded().GetTask())
			case mesos_v1_master.Event_TASK_UPDATED:
				log.Infof("Event_SUBSCRIBED %v", e.GetTaskUpdated().GetState())
			default:
				log.Infof("event type: %v", eventType)
			}
		}
	}

	<- stop
	log.Info("Stop Mesos controller")
}

// AppendServiceHandler implements a service catalog operation
func (c *Controller) AppendServiceHandler(f func(*model.Service, model.Event)) error {
	log.Info("AppendServiceHandler ")

	return nil
}

// AppendInstanceHandler implements a service catalog operation
func (c *Controller) AppendInstanceHandler(f func(*model.ServiceInstance, model.Event)) error {
	log.Info("AppendInstanceHandler ")

	return nil
}

// GetIstioServiceAccounts implements model.ServiceAccounts operation TODO
func (c *Controller) GetIstioServiceAccounts(hostname model.Hostname, ports []int) []string {
	// Need to get service account of service registered with consul
	// Currently Consul does not have service account or equivalent concept
	// As a step-1, to enabling istio security in Consul, We assume all the services run in default service account
	// This will allow all the consul services to do mTLS
	// Follow - https://goo.gl/Dt11Ct


	return nil
}

