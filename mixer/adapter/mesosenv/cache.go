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
		PodTask(string, int) (*TaskInfo, bool)
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
	// IP is containerIP
	ContainerIP net.IP `json:"IP"`
	HostIP      net.IP `json:"hostIP"`
	// ContainerName to its list of host ports
	ContainerPortList map[string][]int `json:"containerPortList"`
}

// Used for the GenerateMesosAttributes
type TaskInfo struct {
	PodName       string            `json:"containerName"`
	ContainerName string            `json:"containerName"`
	Labels        map[string]string `json:"labels"`
	// IP is containerIP
	ContainerIP net.IP `json:"IP"`
	HostIP      net.IP `json:"hostIP"`
}

func newCacheController(client marathon.Marathon, refreshDuration time.Duration, env adapter.Env) (cacheController, error) {
	// Subscribe to Marathon's events
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
				c.env.Logger().Debugf("Stop Pod: %v", podName)
				c.Lock()
				delete(c.podMap, podName)
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

func (c *controllerImpl) PodTask(uid string, port int) (task *TaskInfo, exists bool) {
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
	podName := parts[len(parts)-2]

	c.RLock()
	defer c.RUnlock()
	pod, exists := c.podMap["/"+podName]
	if !exists {
		c.env.Logger().Warningf("Cannot find pod! UID: %v, podID: %v", uid, podName)
		return
	}

	task = &TaskInfo{
		PodName: podName,
		Labels:  pod.Labels,
	}
	taskInst := c.taskMap[taskID]
	if taskInst != nil {
		task.ContainerIP = taskInst.ContainerIP
		task.HostIP = taskInst.HostIP
		// TODO: support multiple containers
		for con := range taskInst.ContainerPortList {
			task.ContainerName = con
		}
	}
	return
}

func (c *controllerImpl) deleteTask(statusUpdate *marathon.EventStatusUpdate) {
	c.Lock()
	defer c.Unlock()
	lastIndex := strings.LastIndex(statusUpdate.TaskID, ".")
	if lastIndex <= 0 || lastIndex >= len(statusUpdate.TaskID)-1 {
		return
	}
	taskID := statusUpdate.TaskID[0:lastIndex]
	containerName := statusUpdate.TaskID[lastIndex+1:]

	task := c.taskMap[taskID]
	if task == nil {
		return
	}
	if len(task.ContainerPortList) <= 1 {
		// Only one container of the pod left, delete it
		delete(c.taskMap, taskID)
	} else {
		// Multiple containers of the pod alive, delete the container first
		delete(task.ContainerPortList, containerName)
	}
}

func (c *controllerImpl) addTask(statusUpdate *marathon.EventStatusUpdate) {
	if len(statusUpdate.Ports) == 0 {
		// Skip containers without host ports
		return
	}
	c.Lock()
	defer c.Unlock()
	_, exist := c.podMap[statusUpdate.AppID]
	if !exist {
		// the pod does not exist in the map. Skip it.
		return
	}

	lastIndex := strings.LastIndex(statusUpdate.TaskID, ".")
	if lastIndex <= 0 || lastIndex >= len(statusUpdate.TaskID)-1 {
		c.env.Logger().Warningf("How could this possible? %v", statusUpdate)
		return
	}
	taskID := statusUpdate.TaskID[0:lastIndex]
	containerName := statusUpdate.TaskID[lastIndex+1:]

	task := c.taskMap[taskID]
	if task == nil {
		task = &TaskInstance{
			HostIP:            net.ParseIP(statusUpdate.Host),
			ContainerPortList: make(map[string][]int),
		}
		if len(statusUpdate.IPAddresses) > 0 {
			task.ContainerIP = net.ParseIP(statusUpdate.IPAddresses[0].IPAddress)
		}
		c.taskMap[taskID] = task
	}
	task.ContainerPortList[containerName] = statusUpdate.Ports

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
		taskInst := TaskInstance{
			HostIP:            net.ParseIP(inst.AgentHostname),
			ContainerIP:       net.ParseIP(containerIP),
			ContainerPortList: make(map[string][]int),
		}
		for _, con := range inst.Containers {
			if len(con.Endpoints) == 0 {
				continue
			}
			taskInst.ContainerPortList[con.Name] = make([]int, len(con.Endpoints))
			for i, ep := range con.Endpoints {
				taskInst.ContainerPortList[con.Name][i] = ep.AllocatedHostPort
			}
		}
		c.taskMap[inst.ID] = &taskInst
		c.env.Logger().Debugf("New Inst %v: %v", inst.ID, taskInst)
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
