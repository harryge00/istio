package mesosenv

import (
	"encoding/json"
	"github.com/harryge00/go-marathon"
	"istio.io/istio/mixer/pkg/adapter"
	"istio.io/istio/pilot/pkg/model"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	ISTIO_SERVICE_LABEL = "istio"
)

type (
	// internal interface used to support testing
	cacheController interface {
		Run(<-chan struct{})
		PodTask(string) (*TaskInfo, bool)
	}

	controllerImpl struct {
		sync.RWMutex
		// podMap stores podName ==> podInfo
		podMap map[string]*PodInfo
		// taskName to TaskInstance
		taskMap          map[string]*TaskInstance
		env              adapter.Env
		client           marathon.Marathon
		depInfoChan      marathon.EventsChannel
		statusUpdateChan marathon.EventsChannel
	}
)

// Abstract model for Marathon pods.
type PodInfo struct {
	// From container names to lists of containerPorts.
	// In Mesos, hostPort may be dynamically allocated. But containerPort is fixed.
	// So applications use hostname:containerPort to communicate with each other.
	Containers map[string]model.PortList `json:"containerPortMap"`

	// Labels from pod specs.
	Labels map[string]string `json:"labels"`
}

// TaskInstance is abstract model for a task instance.
type TaskInstance struct {
	ContainerName string `json:"containerName"`
	// IP is containerIP
	ContainerIP net.IP `json:"IP"`
	HostIP      net.IP `json:"hostIP"`
	// HostPort list
	HostPorts []int `json:"hostports"`
}

// Used for the GenerateMesosAttributes
type TaskInfo struct {
	ContainerName string            `json:"containerName"`
	Labels        map[string]string `json:"labels"`
	// IP is containerIP
	ContainerIP net.IP `json:"IP"`
	HostIP      net.IP `json:"hostIP"`
}

func newCacheController(client marathon.Marathon, refreshDuration time.Duration, env adapter.Env) (cacheController, error) {
	// Register for events
	depInfoChan, err := client.AddEventsListener(marathon.EventIDDeploymentInfo)
	if err != nil {
		return nil, err
	}
	statusUpdateChan, err := client.AddEventsListener(marathon.EventIDStatusUpdate)
	if err != nil {
		return nil, err
	}
	ctrl := &controllerImpl{
		env:              env,
		client:           client,
		depInfoChan:      depInfoChan,
		statusUpdateChan: statusUpdateChan,
		podMap:           make(map[string]*PodInfo),
		taskMap:          make(map[string]*TaskInstance),
	}
	ctrl.syncMarathon()
	return ctrl, nil
}

func (c *controllerImpl) Run(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			c.env.Logger().Infof("Exiting the loop")
			break
		case event := <-c.depInfoChan:
			// deployment_info event
			var depInfo *marathon.EventDeploymentInfo
			depInfo = event.Event.(*marathon.EventDeploymentInfo)
			if depInfo.CurrentStep == nil {
				c.env.Logger().Warningf("Illegal depInfo: %v", depInfo)
				continue
			}
			actions := depInfo.CurrentStep.Actions
			if len(actions) == 0 {
				c.env.Logger().Warningf("Illegal depInfo: %v", depInfo)
				continue
			} else if actions[0].Pod == "" {
				// Not a pod event, skip
				continue
			}

			podName := actions[0].Pod

			switch actions[0].Action {
			// TODO: supports graceful shutdown
			// Delete a pod from podMap
			case "StopPod":
				pod := podName
				c.env.Logger().Debugf("Stop Pod: %v", pod)
				c.Lock()
				delete(c.podMap, pod)
				c.env.Logger().Debugf("podMap: %v", c.podMap)
				c.Unlock()
			case "StartPod":
				// Add a new pod to podMap
				podInfo := c.getPodFromDepInfo(depInfo.Plan.Target.Pods, podName)
				if podInfo != nil {
					c.Lock()
					c.podMap[podName] = podInfo
					c.env.Logger().Debugf("Add pod %v", podName)
					c.printPodMap(c.podMap)
					c.Unlock()
				}
			case "RestartPod":
				// Update a existing pod in podMap
				podInfo := c.getPodFromDepInfo(depInfo.Plan.Target.Pods, podName)
				if podInfo == nil {
					continue
				}
				c.Lock()
				if c.podMap[podName] == nil {
					c.env.Logger().Warningf("Pod %s restarted, but no record in podMap: %v", podName, c.podMap)
					c.podMap[podName] = podInfo
				} else {
					c.podMap[podName].Containers = podInfo.Containers
					c.podMap[podName].Labels = podInfo.Labels
					c.env.Logger().Debugf("Update %v in podMap: %v", podName, c.podMap)
				}
				c.Unlock()
			case "ScalePod":
				c.env.Logger().Debugf("Scaling pod: %v", actions[0])
			}
		case event := <-c.statusUpdateChan:
			// status_update_event
			var statusUpdate *marathon.EventStatusUpdate
			statusUpdate = event.Event.(*marathon.EventStatusUpdate)
			switch statusUpdate.TaskStatus {
			case "TASK_RUNNING":
				c.addTask(statusUpdate)
			case "TASK_KILLED":
				c.deleteTask(statusUpdate)
			}
		}
	}
	c.client.RemoveEventsListener(c.depInfoChan)
	c.client.RemoveEventsListener(c.statusUpdateChan)
	return
}

func (c *controllerImpl) PodTask(uid string) (task *TaskInfo, exists bool) {
	if !strings.HasPrefix(uid, kubePrefix) {
		// Not a valid taskID for mesos
		return
	}
	taskID := strings.TrimPrefix(uid, kubePrefix)
	parts := strings.Split(taskID, ".")
	if len(parts) < 2 {
		c.env.Logger().Warningf("Illegal UID: %v", uid)
		return
	}
	podID := parts[len(parts)-2]
	c.RLock()
	defer c.RUnlock()
	pod, exists := c.podMap[podID]
	if !exists {
		c.env.Logger().Warningf("Pod doesn't exists! UID: %v", uid)
		return
	}
	task = &TaskInfo{
		Labels: pod.Labels,
	}
	taskInst := c.taskMap[taskID]
	if taskInst != nil {
		task.ContainerIP = taskInst.ContainerIP
		task.HostIP = taskInst.HostIP
	}
	return
}

func (c *controllerImpl) deleteTask(statusUpdate *marathon.EventStatusUpdate) {
	c.Lock()
	defer c.Unlock()
	delete(c.taskMap, statusUpdate.TaskID)
}

func (c *controllerImpl) addTask(statusUpdate *marathon.EventStatusUpdate) {
	c.Lock()
	defer c.Unlock()
	_, exist := c.podMap[statusUpdate.AppID]
	if !exist {
		// the pod does not exist in the map. Skip it.
		return
	}

	task := TaskInstance{
		HostIP: net.ParseIP(statusUpdate.Host),
	}
	if len(statusUpdate.IPAddresses) > 0 {
		task.ContainerIP = net.ParseIP(statusUpdate.IPAddresses[0].IPAddress)
	}
	task.HostPorts = statusUpdate.Ports

	c.taskMap[statusUpdate.TaskID] = &task
	c.env.Logger().Infof("Added a task: %v", task)
}

// sync Pods and Tasks from Marathon and cache them in local map.
func (c *controllerImpl) syncMarathon() error {
	c.Lock()
	defer c.Unlock()
	pods, err := c.client.Pods()
	if err != nil {
		return err
	}
	for _, pod := range pods {
		podStatus, err := c.client.PodStatus(pod.ID)
		if err != nil {
			c.env.Logger().Errorf("Failed to get pod %v status: %v", pod.ID, err)
			continue
		}
		c.syncPodStatus(podStatus)
	}
	return nil
}

func (c *controllerImpl) syncPodStatus(status *marathon.PodStatus) {
	svcNames := status.Spec.Labels[ISTIO_SERVICE_LABEL]
	if svcNames == "" {
		return
	}

	podInfo := &PodInfo{
		Labels:     status.Spec.Labels,
		Containers: make(map[string]model.PortList),
	}

	var portList model.PortList
	for _, con := range status.Spec.Containers {
		for _, ep := range con.Endpoints {
			port := model.Port{
				Name:     ep.Name,
				Protocol: convertProtocol(ep.Protocol),
				Port:     ep.ContainerPort,
			}
			portList = append(portList, &port)
		}
		podInfo.Containers[con.Name] = portList
	}
	c.podMap[status.ID] = podInfo
	c.env.Logger().Debugf("ID: %v, podInfo %v", status.ID, podInfo)

	for _, inst := range status.Instances {
		var containerIP string
		// Get container IP
		for _, net := range inst.Networks {
			if len(net.Addresses) > 0 {
				containerIP = net.Addresses[0]
			}
		}
		for _, con := range inst.Containers {
			taskInst := TaskInstance{
				HostIP:      net.ParseIP(inst.AgentHostname),
				ContainerIP: net.ParseIP(containerIP),
			}
			taskInst.HostPorts = make([]int, len(con.Endpoints))
			for i, ep := range con.Endpoints {
				taskInst.HostPorts[i] = ep.AllocatedHostPort
			}
			c.taskMap[con.ContainerID] = &taskInst
			c.env.Logger().Debugf("New Inst:%v", taskInst)
		}
	}

	return
}

// Get PodInfo based on pod spec.
// No task info for now.
func (c *controllerImpl) getPodFromDepInfo(pods []*marathon.Pod, podName string) *PodInfo {
	for _, pod := range pods {
		if pod.ID == podName {
			if pod.Labels[ISTIO_SERVICE_LABEL] == "" {
				c.env.Logger().Infof("No need to process the pod: %v", pod)
				return nil
			}
			return convertPod(pod)
		}
	}
	return nil
}

func convertPod(pod *marathon.Pod) *PodInfo {
	info := PodInfo{
		Labels:     pod.Labels,
		Containers: make(map[string]model.PortList),
	}
	for _, con := range pod.Containers {
		var portList model.PortList
		for _, ep := range con.Endpoints {
			port := model.Port{
				Name:     ep.Name,
				Protocol: convertProtocol(ep.Protocol),
				Port:     ep.ContainerPort,
			}
			portList = append(portList, &port)
		}
		info.Containers[con.Name] = portList
	}
	return &info
}

// TODO: use TCP/UDP or other protocol
func convertProtocol(protocols []string) model.Protocol {
	return model.ProtocolHTTP
}

func (c *controllerImpl) printPodMap(podMap map[string]*PodInfo) {
	c.env.Logger().Debugf("printPodMap")
	for k := range podMap {
		info := podMap[k]
		val, _ := json.Marshal(info)
		c.env.Logger().Infof("pod %v: %v", k, string(val))
	}
}
