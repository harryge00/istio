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
	"encoding/json"
	"github.com/gambol99/go-marathon"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pkg/log"
	"strings"
	"sync"

	"fmt"
	"sort"
)

var (
	DomainSuffix string
)

type MesosCall struct {
	Type string `json:"type"`
}

// ControllerOptions stores the configurable attributes of a Controller.
type ControllerOptions struct
{
	// FQDN Suffix of Container ip. Default "marathon.containerip.dcos.thisdcos.directory"
	// For overlay network, IP like "9.0.x.x" will be used by dcos-net.
	ContainerDomain    string
	// FQDN suffix for vip. Default ".marathon.l4lb.thisdcos.directory"
	VIPDomain	string
	// FQDN suffix for agent IP. Default "marathon.agentip.dcos.thisdcos.directory"
	AgentDoamin string
	Master string
}

// Controller communicates with Consul and monitors for changes
type Controller struct {
	sync.RWMutex
	// podMap stores podName ==> podInfo
	/*
	/group/nginx-pod ->
	{
		front-service: [{9080, "tcp"}, {9001, "udp"}]
		{
			instance-abc-1: "9.0.0.1",
			instance-abc-2: "9.0.0.2",
		}
	}
 	*/
	podMap map[string]*PodInfo

	client marathon.Marathon
	eventChan marathon.EventsChannel
	depSuccessChan marathon.EventsChannel
	depInfoChan marathon.EventsChannel
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
	//events, err := client.AddEventsListener(marathon.EventIDApplications)
	//if err != nil {
	//	return nil, err
	//}
	depChan, err := client.AddEventsListener(marathon.EventIDDeploymentSuccess)
	if err != nil {
		return nil, err
	}
	depInfoChan, err := client.AddEventsListener(marathon.EventIDDeploymentInfo)
	if err != nil {
		return nil, err
	}
	//podDelChan, err := client.AddEventsListener(marathon.EventIdPodDeleted)
	//if err != nil {
	//	return nil, err
	//}


	c := &Controller{
		client: client,
		podMap:  make(map[string]*PodInfo),
		//eventChan: events,
		depSuccessChan: depChan,
		depInfoChan: depInfoChan,
		//podDeleteChan: podDelChan,
	}

	err = c.initPodmap()
	if err != nil {
		return nil, err
	}
	// TODO: support using other domains
	DomainSuffix = options.VIPDomain
	return c, nil
}

func (c *Controller) initPodmap() error {
	c.Lock()
	defer c.Unlock()
	pods, err := c.client.Pods()
	if err != nil {
		return err
	}
	for _, pod := range pods {
		podStatus, err := c.client.PodStatus(pod.ID)
		if err != nil {
			log.Errorf("Failed to get pod %v status: %v", pod.ID, err)
			continue
		}
		podInfo := getPodInfo(podStatus)
		log.Infof("ID: %v, push podInfo %v", pod.ID, podInfo)
		c.podMap[pod.ID] = podInfo
	}
	return nil
}

// Services list declarations of all services in the system
func (c *Controller) Services() ([]*model.Service, error) {
	c.RLock()
	serviceMap := make(map[model.Hostname]*model.Service)
	for _, pod := range c.podMap {
		for name, _ := range pod.HostNames {
			hostname := serviceHostname(&name)
			service := serviceMap[hostname]
			if service == nil {
				service = &model.Service{
					Hostname:     hostname,
					// TODO: use Marathon-LB address
					Ports:	model.PortList{},
					Address:      "0.0.0.0",
					MeshExternal: false,
					Resolution:   model.ClientSideLB,
					Attributes: model.ServiceAttributes{
						Name:      string(hostname),
						Namespace: model.IstioDefaultConfigNamespace,
					},
				}
			}
			// Append only unique ports
			portExists := make(map[int]bool)
			for _, port := range service.Ports {
				portExists[port.Port] = true
			}
			for _, port := range pod.PortList {
				if !portExists[port.Port] {
					service.Ports = append(service.Ports, port)
					portExists[port.Port] = true
				}
			}
			serviceMap[hostname] = service
		}
	}
	c.RUnlock()
	log.Infof("serviceMap: %v", serviceMap)


	out := make([]*model.Service, 0, len(serviceMap))
	for _, v := range serviceMap {
		out = append(out, v)
	}
	arr := marshalServices(out)
	log.Infof("Services: %v", arr)
	sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })
	return out, nil
}

func marshalServices(svc []*model.Service) []string {
	arr := make([]string, 0, len(svc))
	for _, v := range svc {
		j, _ := json.Marshal(*v)
		arr = append(arr, string(j))
	}
	return arr
}

func marshalServiceInstances(svc []*model.ServiceInstance) []string {
	arr := make([]string, 0, len(svc))
	for _, v := range svc {
		j, _ := json.Marshal(*v)
		arr = append(arr, string(j))
	}
	return arr
}

// GetService retrieves a service by host name if it exists
func (c *Controller) GetService(hostname model.Hostname) (*model.Service, error) {
	out := &model.Service{
		Hostname:     hostname,
		Ports:	model.PortList{},
		Address:      "0.0.0.0",
		MeshExternal: false,
		Resolution:   model.ClientSideLB,
		Attributes: model.ServiceAttributes{
			Name:      string(hostname),
			Namespace: model.IstioDefaultConfigNamespace,
		},
	}
	c.RLock()
	defer c.RUnlock()
	portExist := make(map[int]bool)
	for _, podInfo := range c.podMap {
		if podInfo.HostNames[string(hostname)] {
			for _, port := range podInfo.PortList {
				if !portExist[port.Port] {
					portExist[port.Port] = true
					out.Ports = append(out.Ports, port)
				}

			}
		}
	}

	j, _ := json.Marshal(*out)
	log.Infof("GetService hostname %v: %v", hostname, string(j))

	return out, nil
}

// ManagementPorts retrieves set of health check ports by instance IP.
// This does not apply to Consul service registry, as Consul does not
// manage the service instances. In future, when we integrate Nomad, we
// might revisit this function.
func (c *Controller) ManagementPorts(addr string) model.PortList {
	log.Info("ManagementPorts not implemented")

	return nil
}

// WorkloadHealthCheckInfo retrieves set of health check info by instance IP.
// This does not apply to Consul service registry, as Consul does not
// manage the service instances. In future, when we integrate Nomad, we
// might revisit this function.
func (c *Controller) WorkloadHealthCheckInfo(addr string) model.ProbeList {
	log.Info("WorkloadHealthCheckInfo not implemented")
	return nil
}

// InstancesByPort retrieves instances for a service that match
// any of the supplied labels. All instances match an empty tag list.
func (c *Controller) InstancesByPort(hostname model.Hostname, port int,
	labels model.LabelsCollection) ([]*model.ServiceInstance, error) {
	svcName, err := parseHostname(hostname)
	if err != nil {
		log.Infof("parseHostname(%s) => error %v", hostname, err)
		return nil, err
	}
	instances := []*model.ServiceInstance{}

	c.RLock()
	for _, pod := range c.podMap {
		if portMatch(pod.PortList, port) && pod.HostNames[svcName] && labels.HasSubsetOf(pod.Labels) {
			//log.Infof("port matched: %v : %v", name, port)
			instances = append(instances, getInstancesOfPod(svcName, port, pod)...)
		}
	}
	c.RUnlock()
	arr := marshalServiceInstances(instances)
	log.Infof("InstancesByPort hostname: %v, port: %v, labels: %v -> %v", hostname, port, labels, arr)

	return instances, nil
}

func getInstancesOfPod(svc string, reqSvcPort int, pod *PodInfo) []*model.ServiceInstance {
	out := make([]*model.ServiceInstance, 0)

	hostName := serviceHostname(&svc)

	for _, inst := range pod.InstanceMap {
		service := model.Service{
			Hostname:     hostName,
			// TODO: use marathon-lb address
			Address:      inst.IP,
			Ports:        pod.PortList,
			MeshExternal: false,
			Resolution:   model.ClientSideLB,
			Attributes: model.ServiceAttributes{
				Name:      string(hostName),
				Namespace: model.IstioDefaultConfigNamespace,
			},
		}
		for svcPort, hostPort := range inst.PortMapping {
			if svcPort == reqSvcPort {
				inst := model.ServiceInstance{
					Endpoint: model.NetworkEndpoint{
						Address:     inst.IP,
						Port:        hostPort.Port,
						ServicePort: &model.Port{
							Name: hostPort.Name,
							Protocol: hostPort.Protocol,
							Port: svcPort,
						},
					},
					//AvailabilityZone: "default",
					Service: &service,
					Labels: pod.Labels,
				}
				out = append(out, &inst)
			}
		}
	}
	return out
}


// returns true if an instance's port matches with any in the provided list
func portMatch(portList model.PortList, servicePort int) bool {
	if servicePort == 0 {
		return true
	}
	for _, port := range portList {
		if port.Port == servicePort {
			return true
		}
	}
	return false
}

// GetProxyServiceInstances lists service instances co-located with a given proxy
func (c *Controller) GetProxyServiceInstances(node *model.Proxy) ([]*model.ServiceInstance, error) {
	out := make([]*model.ServiceInstance, 0)
	log.Infof("GetProxyServiceInstances: %v", node)
	c.RLock()
	defer c.RUnlock()
	for _, pod := range c.podMap {
		for id, inst := range pod.InstanceMap {
			if id == node.ID {
				// serviceMap -> hostName
				log.Infof("Find instance %v: %v", id, inst)
				out = append(out, convertTaskInstance(pod, inst)...)
			}
		}
	}

	arr := marshalServiceInstances(out)
	log.Infof("GetProxyServiceInstances %v: %v, result: %v", node, arr, out)

	return out, nil
}

// Instances retrieves instances for a service and its ports that match
// any of the supplied labels. All instances match an empty tag list.
func (c *Controller) Instances(hostname model.Hostname, ports []string,
	labels model.LabelsCollection) ([]*model.ServiceInstance, error) {
	return nil, fmt.Errorf("NOT IMPLEMENTED")
}


func convertTaskInstance(podInfo *PodInfo, inst *TaskInstance) []*model.ServiceInstance {
	out := []*model.ServiceInstance{}

	for hostName := range podInfo.HostNames {
		service := model.Service{
			Hostname:     model.Hostname(hostName),
			Address:      inst.IP,
			Ports:        podInfo.PortList,
			MeshExternal: false,
			Resolution:   model.ClientSideLB,
			Attributes: model.ServiceAttributes{
				Name:      hostName,
				Namespace: model.IstioDefaultConfigNamespace,
			},
		}

		// Here svcPort is used of accessing services
		// whereas hostPort is regarded as endpoints port
		for svcPort, hostPort := range inst.PortMapping {
			epPort := model.Port{
				Name: hostPort.Name,
				Port: svcPort,
				// TODO: Use other protocol
				Protocol: hostPort.Protocol,
			}
			out = append(out, &model.ServiceInstance{
				Endpoint: model.NetworkEndpoint{
					Address:     inst.IP,
					Port:        hostPort.Port,
					ServicePort: &epPort,
				},
				//AvailabilityZone: "default",
				Service: &service,
				Labels: podInfo.Labels,
			})

		}
	}
	return out
}

func (c *Controller) Run(stop <-chan struct{}) {
	for {
		select {
		case <- stop:
			log.Info("Exiting the loop")
		case event := <-c.depInfoChan:
			var depInfo *marathon.EventDeploymentInfo
			depInfo = event.Event.(*marathon.EventDeploymentInfo)
			if len(depInfo.Plan.Steps) > 0 && len(depInfo.Plan.Steps[0].Actions) > 0 &&
				depInfo.Plan.Steps[0].Actions[0].Action == "StopPod" {
				pod := depInfo.Plan.Steps[0].Actions[0].Pod
				log.Infof("Stop Pod: %v", pod)
				c.Lock()
				delete(c.podMap, pod)
				log.Infof("podMap: %v", c.podMap)
				c.Unlock()
			}

		case event := <-c.depSuccessChan:
			var deployment *marathon.EventDeploymentSuccess
			deployment = event.Event.(*marathon.EventDeploymentSuccess)
			steps := deployment.Plan.Steps
			if len(steps) == 0 {
				log.Warnf("No steps: %v", deployment)
				continue
			}
			actions := steps[0].Actions
			if len(actions) > 0 && actions[0].Pod != "" {
				log.Infof("deployment success: %v", actions)
				switch actions[0].Action {
				case "StopPod":
					log.Infof("stoppod: %v. Pod should have been deleted. Skip", actions[0].Pod)
					continue
					//c.Lock()
					//delete(c.podMap, actions[0].Pod)
					//log.Infof("podMap: %v", c.podMap)
					//c.Unlock()
				default:
					pod := actions[0].Pod
					podStatus, err := c.client.PodStatus(pod)
					if err != nil {
						log.Errorf("Failed to get pod %v status: %v", pod, err)
						continue
					}
					podInfo := getPodInfo(podStatus)
					log.Infof("podInfo %v: %v", pod, podInfo)
					c.Lock()
					c.podMap[pod] = podInfo
					c.Unlock()
				}
			}
		}
	}

	c.client.RemoveEventsListener(c.depSuccessChan)
	//c.client.RemoveEventsListener(c.eventChan)
	//c.client.RemoveEventsListener(c.podDeleteChan)
}

// Endpoint is like mesos endpoints.
// Portmapping between service ports and container ports.
type Endpoint struct {
	ContainerPort	int
	Name string
	Protocol model.Protocol
}

const (
	ISTIO_SERVICE_LABEL = "ISTIO_SERVICE_NAME"
)

// 80 /abc:8080 /abc:9191
// 8181 /abc:8080 /def:8787
type PodInfo struct {
	PortList model.PortList
	// A pod may have multiple hostnames
	HostNames map[string]bool
	InstanceMap map[string]*TaskInstance
	Labels map[string]string
}

// TaskInstance is abstract model for a task instance.
type TaskInstance struct {
	IP string
	PortMapping map[int]*model.Port
}

func getPodInfo(status *marathon.PodStatus) *PodInfo {
	svcNames := status.Spec.Labels[ISTIO_SERVICE_LABEL]
	svcs := strings.Split(svcNames, ",")
	hostMap := make(map[string]bool)
	for _, svc := range svcs {
		hostName := fmt.Sprintf("%s.%s", svc, DomainSuffix)
		hostMap[hostName] = true
	}
	podInfo := &PodInfo{
		HostNames: hostMap,
		InstanceMap: make(map[string]*TaskInstance),
		Labels: status.Spec.Labels,
	}

	// A map of container ports
	portMap := make(map[string]*model.Port)
	for _, con := range status.Spec.Containers {
		for _, ep := range con.Endpoints {
			port := model.Port{
				Name: ep.Name,
				Protocol: convertProtocol(ep.Protocol),
				Port: ep.ContainerPort,
			}
			portMap[ep.Name] = &port
			podInfo.PortList = append(podInfo.PortList, &port)
		}
	}

	//podInfo.ContainerPorts = portMap

	for _, inst := range status.Instances {
		taskInst := TaskInstance{
			// TODO: make sure AgentHostName is a IP!
			IP: inst.AgentHostname,
			PortMapping: make(map[int]*model.Port),
		}
		for _, con := range inst.Containers {
			for _, ep := range con.Endpoints {
				containerPort := portMap[ep.Name]
				// From container port to allocated hostPort.
				taskInst.PortMapping[containerPort.Port] = &model.Port{
					Name: ep.Name,
					Port: ep.AllocatedHostPort,
					Protocol: containerPort.Protocol,
				}
			}

		}
		podInfo.InstanceMap[inst.ID] = &taskInst
	}
	return podInfo
}

// TODO: use TCP/UDP or other protocol
func convertProtocol(protocols []string) model.Protocol {
	return model.ProtocolHTTP
}

// parseHostname extracts service name from the service hostname
func parseHostname(hostname model.Hostname) (name string, err error) {
	parts := strings.Split(string(hostname), ".")
	if len(parts) < 1 || parts[0] == "" {
		err = fmt.Errorf("missing service name from the service hostname %q", hostname)
		return
	}
	name = parts[0]
	return
}

func serviceHostname(name *string) model.Hostname {
	return model.Hostname(fmt.Sprintf("%s.%s", *name, DomainSuffix))
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
func (c *Controller) GetIstioServiceAccounts(hostname model.Hostname, ports []string) []string {

		// Need to get service account of service registered with consul
	// Currently Consul does not have service account or equivalent concept
	// As a step-1, to enabling istio security in Consul, We assume all the services run in default service account
	// This will allow all the consul services to do mTLS
	// Follow - https://goo.gl/Dt11Ct


	return []string{
		"spiffe://cluster.local/ns/default/sa/default",
	}
}

