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
	"strconv"
	"strings"
	"time"

	"github.com/gambol99/go-marathon"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/log"
	"sync"


	"fmt"
	"github.com/mesos/go-proto/mesos/v1"
	"sort"
)

var (
	DomainSuffix string
)

type MesosCall struct {
	Type string `json:"type"`
}

// ControllerOptions stores the configurable attributes of a Controller.
type ControllerOptions struct {
	// FQDN Suffix of Container ip. Default "marathon.containerip.dcos.thisdcos.directory"
	// For overlay network, IP like "9.0.x.x" will be used by dcos-net.
	ContainerDomain    string
	// FQDN suffix for vip. Default ".marathon.l4lb.thisdcos.directory"
	VIPDomain	string
	// FQDN suffix for agent IP. Default "marathon.agentip.dcos.thisdcos.directory"
	AgentDoamin string
	Master string
	Timeout  time.Duration
}

// Controller communicates with Consul and monitors for changes
type Controller struct {
	sync.RWMutex
	// podMap stores hostname ==> service, it is used to reduce convertService calls.
	podMap map[string]*PodInfo

	client marathon.Marathon
	eventChan marathon.EventsChannel
	deploymentsChan marathon.EventsChannel
	podDeleteChan marathon.EventsChannel
}

// NewController creates a new Consul controller
func NewController(options ControllerOptions) (*Controller, error) {
	log.Infof("Mesos options: %v", options)

	config := marathon.NewDefaultConfig()
	config.URL = options.Master
	config.EventsTransport = marathon.EventsTransportSSE
	log.Infof("Creating a client, Marathon: %s", config.URL)

	client, err := marathon.NewClient(config)
	if err != nil {
		return nil, err
	}

	// Register for events
	events, err := client.AddEventsListener(marathon.EventIDApplications)
	if err != nil {
		return nil, err
	}
	deployments, err := client.AddEventsListener(marathon.EventIDDeploymentSuccess)
	if err != nil {
		return nil, err
	}
	podDelChan, err := client.AddEventsListener(marathon.EventIdPodDeleted)
	if err != nil {
		return nil, err
	}


	// TODO: support using other domains
	DomainSuffix = options.VIPDomain
	return &Controller{
		client: client,
		podMap:  make(map[string]*PodInfo),
		eventChan: events,
		deploymentsChan: deployments,
		podDeleteChan: podDelChan,
	}, nil
}

// Services list declarations of all services in the system
func (c *Controller) Services() ([]*model.Service, error) {
	log.Info("GetServices")
	c.RLock()
	out := make([]*model.Service)
	for name, pod := range c.podMap {
		for lb, ports := range pod.LBPorts {
			service := &model.Service{
				Hostname:     hostname,
				Address:      "0.0.0.0",
				Ports:        svcPorts,
				MeshExternal: meshExternal,
				Resolution:   resolution,
				Attributes: model.ServiceAttributes{
					Name:      string(hostname),
					Namespace: model.IstioDefaultConfigNamespace,
				},
			}
		}

	}
	c.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })

	return out, nil
}

// GetService retrieves a service by host name if it exists
func (c *Controller) GetService(hostname model.Hostname) (*model.Service, error) {
	c.RLock()
	defer c.RUnlock()
	log.Debugf("GetService hostname: %v -> %v", hostname, c.podMap[""])
	return nil, nil
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


func (c *Controller) Run(stop <-chan struct{}) {
	for {
		select {
		case <- stop:
			log.Info("Exiting the loop")

		case event := <-c.deploymentsChan:
			var deployment *marathon.EventDeploymentSuccess
			deployment = event.Event.(*marathon.EventDeploymentSuccess)
			log.Infof("deployment success: %v", deployment.Plan)
			steps := deployment.Plan.Steps
			if len(steps) == 0 {
				log.Warnf("No steps: %v", deployment)
				continue
			}
			actions := steps[0].Actions
			if len(actions) > 0 && actions[0].Pod != "" {
				switch actions[0].Action {
				case "StopPod":
					log.Infof("stoppod: %v", actions[0].Pod)
					c.Lock()
					delete(c.podMap, actions[0].Pod)
					log.Infof("podMap: %v", c.podMap)
					c.Unlock()
				default:
					pod := actions[0].Pod
					podStatus, err := c.client.PodStatus(pod)
					if err != nil {
						log.Errorf("Failed to get pod %v status: %v", pod, err)
						continue
					}
					podInfo := getPodInfo(podStatus)
					log.Infof("podInfo %v", podInfo)
					c.Lock()
					c.podMap[pod] = podInfo
					c.Unlock()
				}
			}
		//case event := <-c.podDeleteChan:
		//	var podDeleted *marathon.EventPodDeleted
		//	podDeleted = event.Event.(*marathon.EventPodDeleted)
		//	log.Infof("pod deleted: %v", podDeleted.URI)

		}
	}

	c.client.RemoveEventsListener(c.deploymentsChan)
	//c.client.RemoveEventsListener(c.eventChan)
	//c.client.RemoveEventsListener(c.podDeleteChan)
}

type Port struct {
	Addr string
	Port int
	Protocol model.Protocol
}

type PodInfo struct {
	// endpoint name to port list
	LBPorts map[string][]Port
	InstanceIPMap map[string]string
	Labels map[string]string
}

func parseVIP(s string) (addr string, port int, err error) {
	arr := strings.Split(s[1:], ":")
	if len(arr) == 2 {
		addr = arr[0]
		port, err = strconv.Atoi(arr[1])
	} else {
		err = fmt.Errorf("Illegal vip label: %v", s)
	}
	return
}

/*
	nginx-pod ->
	{
		front-service: [{9080, "tcp"}, {9001, "udp"}]
		{
			instance-abc-1: "9.0.0.1",
			instance-abc-2: "9.0.0.2",
		}
	}
 */
func getPodInfo(status *marathon.PodStatus) *PodInfo {
	lbports := make(map[string][]Port)
	for _, con := range status.Spec.Containers {
		for _, ep := range con.Endpoints {
			for k, v := range ep.Labels {
				if strings.HasPrefix(k, "VIP_") && strings.HasPrefix(v, "/") {
					addr, port, err := parseVIP(v)
					ports := lbports[addr]
					if err != nil {
						log.Errorf("parseVIP %v: %v", v, err)
						continue
					}
					for i := range ep.Protocol {
						ports = append(ports, Port{
							Port: port,
							Protocol: model.ParseProtocol(ep.Protocol[i]),
						})
					}
					lbports[addr] = ports
				}
			}
		}
	}
	podInfo := &PodInfo{
		Labels: status.Spec.Labels,
		LBPorts: lbports,
		InstanceIPMap: make(map[string]string),
	}
	for _, inst := range status.Instances {
		// TODO: make sure only overlay IP is used.
		if len(inst.Networks) > 0 && len(inst.Networks[0].Addresses) > 0 {
			podInfo.InstanceIPMap[inst.ID] = inst.Networks[0].Addresses[0]
		}
	}

	return podInfo
}


func serviceHostname(name *string) model.Hostname {
	return model.Hostname(fmt.Sprintf("%s.%s", *name, DomainSuffix))
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

