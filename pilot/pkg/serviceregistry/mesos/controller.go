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
	"context"

	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/log"
	"sync"
	"github.com/mesos/go-proto/mesos/v1/master"

	"github.com/miroswan/mesops/pkg/v1"

	"istio.io/istio/pilot/pkg/serviceregistry/kube"
	"sort"
	"github.com/mesos/go-proto/mesos/v1"
	"fmt"
)

type MesosCall struct {
	Type string `json:"type"`
}

// Controller communicates with Consul and monitors for changes
type Controller struct {
	// ClusterID identifies the remote cluster in a multicluster env.
	ClusterID string

	sync.RWMutex
	// servicesMap stores hostname ==> service, it is used to reduce convertService calls.
	servicesMap map[model.Hostname]*model.Service

	client *v1.Master
}

// NewController creates a new Consul controller
func NewController(serverURL string, options kube.ControllerOptions) (*Controller, error) {
	log.Infof("serverURL: %v, options: %v", serverURL, options)

	client, err := v1.NewMasterBuilder(serverURL).Build()
	if err != nil {
		return nil, err
	}

	return &Controller{
		client: client,
		servicesMap:  make(map[model.Hostname]*model.Service),
	}, nil
}

// Services list declarations of all services in the system
func (c *Controller) Services() ([]*model.Service, error) {
	log.Info("GetServices")
	c.RLock()
	out := make([]*model.Service, 0, len(c.servicesMap))
	for hostname, svc := range c.servicesMap {
		log.Debugf("%s, %v", hostname, svc)
		out = append(out, svc)
	}
	c.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })

	return out, nil
}

// GetService retrieves a service by host name if it exists
func (c *Controller) GetService(hostname model.Hostname) (*model.Service, error) {
	c.RLock()
	defer c.RUnlock()
	log.Debugf("GetService hostname: %v -> %v", hostname, c.servicesMap[hostname])
	return c.servicesMap[hostname], nil
}

// ManagementPorts retrieves set of health check ports by instance IP.
// This does not apply to Consul service registry, as Consul does not
// manage the service instances. In future, when we integrate Nomad, we
// might revisit this function.
func (c *Controller) ManagementPorts(addr string) model.PortList {
	log.Info("ManagementPorts")

	return nil
}

// WorkloadHealthCheckInfo retrieves set of health check info by instance IP.
// This does not apply to Consul service registry, as Consul does not
// manage the service instances. In future, when we integrate Nomad, we
// might revisit this function.
func (c *Controller) WorkloadHealthCheckInfo(addr string) model.ProbeList {
	log.Info("WorkloadHealthCheckInfo")
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
	go func() {
		err := c.client.Subscribe(context.TODO(), es)
		if err != nil {
			log.Fatalf("Failed to subscribe Mesos-operator API: %v", err)
		}
	}()

	for {
		select {
		case <-stop:
			break
		case e := <-es:
			eventType := e.GetType()
			switch eventType {
			case mesos_v1_master.Event_SUBSCRIBED:
				log.Infof("Event_SUBSCRIBED %v", e.GetSubscribed().GetGetState())
			case mesos_v1_master.Event_TASK_ADDED:
				task := e.GetTaskAdded().GetTask()
				log.Infof("Event_TASK_ADDED labels:%v, discovery: %v", task.Labels, *task.Discovery)

				log.Infof("Event_TASK_ADDED %v", task)
				service := convertTask(task)
				c.Lock()
				c.servicesMap[service.Hostname] = service
				c.Unlock()
			case mesos_v1_master.Event_TASK_UPDATED:
				task := e.GetTaskUpdated()
				log.Infof("Event_TASK_UPDATED task: %v", task)
				log.Infof("Event_TASK_UPDATED state %v", e.GetTaskUpdated().GetState())
			default:
				log.Infof("event type: %v", eventType)
			}
		}
	}

	log.Info("Stop Mesos controller")
}


func serviceHostname(name *string) model.Hostname {
	return model.Hostname(fmt.Sprintf("%s.marathon.autoip.dcos.thisdcos.directory", *name))
}

func convertTask(task *mesos_v1.Task) *model.Service {
	if task == nil || task.Discovery == nil {
		log.Errorf("Illegal Task: %v", task)
		return nil
	}

	meshExternal := false
	resolution := model.ClientSideLB

	var ports model.PortList
	if task.Discovery.Ports != nil {
		ports = make(model.PortList, len(task.Discovery.Ports.Ports))
		for i, port := range task.Discovery.Ports.Ports {
			ports[i] = &model.Port{
				Name: *port.Name,
				Port: int(*port.Number),
				Protocol: model.Protocol(*port.Protocol),
			}
		}
	} else {
		log.Errorf("Illegal Task Port: %v", task.Discovery)
	}

	svcPorts := make(model.PortList, 0, len(ports))
	for _, port := range ports {
		svcPorts = append(svcPorts, port)
	}

	hostname := serviceHostname(task.Name)
	out := &model.Service{
		Hostname:     hostname,
		Address:      "0.0.0.0",
		Ports:        ports,
		MeshExternal: meshExternal,
		Resolution:   resolution,
		Attributes: model.ServiceAttributes{
			Name:      string(hostname),
			Namespace: model.IstioDefaultConfigNamespace,
		},
	}

	return out
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

