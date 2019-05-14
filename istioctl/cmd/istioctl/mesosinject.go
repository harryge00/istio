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

package main

import (
	"errors"
	"fmt"
	"go.uber.org/multierr"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/harryge00/go-marathon"
	"istio.io/istio/pkg/log"
)

const (
	defaultProxyImage = "hyge/proxy_debug:mesos"
)

var (
	injectMesosCmd = &cobra.Command{
		Use:   "mesos-inject",
		Short: "Inject Envoy sidecar into Mesos pod resources",
		Long: `

mesos-inject manually injects the Envoy sidecar into Kubernetes
workloads. Unsupported resources are left unmodified so it is safe to
run mesos-inject over a single file that contains multiple Service,
ConfigMap, Deployment, etc. definitions for a complex application. Its
best to do this when the resource is initially created.

k8s.io/docs/concepts/workloads/pods/pod-overview/#pod-templates is
updated for Job, DaemonSet, ReplicaSet, Pod and Deployment YAML resource
documents. Support for additional pod-based resource types can be
added as necessary.

The Istio project is continually evolving so the Istio sidecar
configuration may change unannounced. When in doubt re-run istioctl
mesos-inject on deployments to get the most up-to-date changes.

To override the sidecar injection template from kubernetes configmap
'istio-inject', the parameters --injectConfigFile or --injectConfigMapName
can be used. Either of options would typically be used with the
file/configmap created with a new Istio release.
`,
		Example: `
# Update resources on the fly before applying.
kubectl apply -f <(istioctl mesos-inject -f <resource.yaml>)

# Create a persistent version of the deployment with Envoy sidecar
# injected.
istioctl mesos-inject -f deployment.yaml -o deployment-injected.yaml

# Update an existing deployment.
kubectl get deployment -o yaml | istioctl mesos-inject -f - | kubectl apply -f -

# Create a persistent version of the deployment with Envoy sidecar
# injected configuration from Kubernetes configmap 'istio-inject'
istioctl mesos-inject -f deployment.yaml -o deployment-injected.yaml --injectConfigMapName istio-inject
`,
		RunE: func(c *cobra.Command, _ []string) (err error) {
			if err = validateMesosFlags(); err != nil {
				return err
			}

			var reader io.Reader
			if !emitTemplate {
				if inMesosFilename == "-" {
					reader = os.Stdin
				} else {
					var in *os.File
					if in, err = os.Open(inMesosFilename); err != nil {
						return err
					}
					reader = in
					defer func() {
						if errClose := in.Close(); errClose != nil {
							log.Errorf("Error: close file from %s, %s", inMesosFilename, errClose)

							// don't overwrite the previous error
							if err == nil {
								err = errClose
							}
						}
					}()
				}
			}

			//var writer io.Writer
			//if outMesosFilename == "" {
			//	writer = c.OutOrStdout()
			//} else {
			//	var out *os.File
			//	if out, err = os.Create(outMesosFilename); err != nil {
			//		return err
			//	}
			//	writer = out
			//	defer func() {
			//		if errClose := out.Close(); errClose != nil {
			//			log.Errorf("Error: close file from %s, %s", outMesosFilename, errClose)
			//
			//			// don't overwrite the previous error
			//			if err == nil {
			//				err = errClose
			//			}
			//		}
			//	}()
			//}

			//var meshConfig *meshconfig.MeshConfig
			//if meshConfigFile != "" {
			//	if meshConfig, err = cmd.ReadMeshConfig(meshConfigFile); err != nil {
			//		return err
			//	}
			//} else {
			//	if meshConfig, err = getMeshConfigFromConfigMap(kubeconfig); err != nil {
			//		return err
			//	}
			//}
			//
			//var sidecarTemplate string
			//if injectConfigFile != "" {
			//	injectionConfig, err := ioutil.ReadFile(injectConfigFile) // nolint: vetshadow
			//	if err != nil {
			//		return err
			//	}
			//	var config inject.Config
			//	if err := yaml.Unmarshal(injectionConfig, &config); err != nil {
			//		return err
			//	}
			//	sidecarTemplate = config.Template
			//} else {
			//	if sidecarTemplate, err = getInjectConfigFromConfigMap(kubeconfig); err != nil {
			//		return err
			//	}
			//}
			//
			//var valuesConfig string
			//if valuesFile != "" {
			//	valuesConfigBytes, err := ioutil.ReadFile(valuesFile) // nolint: vetshadow
			//	if err != nil {
			//		return err
			//	}
			//	valuesConfig = string(valuesConfigBytes)
			//} else {
			//	if valuesConfig, err = getValuesFromConfigMap(kubeconfig); err != nil {
			//		return err
			//	}
			//}
			//
			//if emitTemplate {
			//	config := inject.Config{
			//		Policy:   inject.InjectionPolicyEnabled,
			//		Template: sidecarTemplate,
			//	}
			//	out, err := yaml.Marshal(&config)
			//	if err != nil {
			//		return err
			//	}
			//	fmt.Println(string(out))
			//	return nil
			//}


			var app marathon.Application
			byteValues, err := ioutil.ReadAll(reader)
			if err != nil {
				return err
			}
			app.UnmarshalJSON(byteValues)

			log.Infof("app: %v", app)

			// Prune "/groups/appA" to "appA"
			lastSlashIndex := strings.LastIndex(app.ID, "/")

			appName := app.ID[lastSlashIndex + 1:]
			pod := marathon.NewPod()

			workLoadContainer := &marathon.PodContainer{
				Name: appName,
				Exec: &marathon.PodExec{
					Command: marathon.PodCommand{
						Shell: *app.Cmd,
					},
				},
				Image: &marathon.PodContainerImage{
					Kind:      "DOCKER",
					ID:        app.Container.Docker.Image,
					ForcePull: *app.Container.Docker.ForcePullImage,
				},
				Resources: &marathon.Resources{
					Cpus: app.CPUs,
					Mem:  *app.Mem,
				},
				Env: *app.Env,
			}

			for _, port := range *app.Container.PortMappings {
				ep := &marathon.PodEndpoint{
					Name: port.Name,
					ContainerPort: port.ContainerPort,
					HostPort: port.HostPort,
					Labels: *port.Labels,
					Protocol: []string{port.Protocol},

				}
				workLoadContainer.Endpoints = append(workLoadContainer.Endpoints, ep)
			}

			podHealthCheck, err := getPodHealthCheck(&app)
			if err != nil {
				return err
			}
			if podHealthCheck != nil {
				workLoadContainer.HealthCheck = podHealthCheck
			}

			for i, vol := range *app.Container.Volumes {
				if vol.HostPath == "" && vol.Persistent == nil {
					// Not a valid pod volume
					continue
				}
				podVol := &marathon.PodVolume{
					Name: fmt.Sprintf("volume-%d", i),
				}
				volMount := &marathon.PodVolumeMount{
					Name: podVol.Name,
				}
				if vol.HostPath != "" {
					podVol.Host = vol.HostPath
					volMount.MountPath = vol.ContainerPath
				} else if vol.Persistent != nil {
					podVol.Persistent = vol.Persistent
					volMount.MountPath = vol.ContainerPath
				}
				pod.AddVolume(podVol)
				workLoadContainer.VolumeMounts = append(workLoadContainer.VolumeMounts, volMount)
			}

			pod.ID = app.ID
			pod.Labels = *app.Labels
			pod.AddNetwork(marathon.NewContainerPodNetwork("dcos"))
			pod.Count(*app.Instances)
			pod.AddContainer(workLoadContainer)
			pod.AddContainer(createProxyContainer())
			podSchedulingPolicy := getPodSchedulingPolicy(&app)
			if podSchedulingPolicy != nil {
				pod.SetPodSchedulingPolicy(podSchedulingPolicy)
			}

			pod.Secrets = *app.Secrets

			podBytes, err := pod.MarshalJSON()
			if err != nil {
				return err
			}
			log.Infof("%v", string(podBytes))

			return nil
		},
	}
)

func createProxyContainer(serviceName string) *marathon.PodContainer{
	podContainer := marathon.PodContainer{
		Name: "istio-proxy",
		Image: &marathon.PodContainerImage{
			Kind:      "DOCKER",
			ID:        defaultProxyImage,
		},
		Resources: &marathon.Resources{
			Cpus: 0.2,
			Mem:  512,
		},
		Env: map[string]string{},
	}
	if os.Getenv("SERVICES_DOMAIN") != "" {
		podContainer.Env["SERVICES_DOMAIN"] = os.Getenv("SERVICES_DOMAIN")
	} else {
		podContainer.Env["SERVICES_DOMAIN"] = "marathon.slave.mesos"
	}

	if os.Getenv("SERVICE_NAME") != "" {
		podContainer.Env["SERVICE_NAME"] = os.Getenv("SERVICE_NAME")
	} else {
		podContainer.Env["SERVICE_NAME"] = serviceName
	}

	if os.Getenv("ZIPKIN_ADDRESS") != "" {
		podContainer.Env["ZIPKIN_ADDRESS"] = os.Getenv("ZIPKIN_ADDRESS")
	} else {
		podContainer.Env["ZIPKIN_ADDRESS"] = "zipkin.istio.marathon.slave.mesos:31767"
	}

	if os.Getenv("DISCOVERY_ADDRESSS") != "" {
		podContainer.Env["DISCOVERY_ADDRESSS"] = os.Getenv("DISCOVERY_ADDRESSS")
	} else {
		podContainer.Env["DISCOVERY_ADDRESSS"] = "pilot.istio.marathon.slave.mesos:31510"
	}


	podContainer.Env["SERVICES_DOMAIN"] = "marathon.slave.mesos"
	podContainer.Env["SERVICES_DOMAIN"] = "marathon.slave.mesos"
	return &podContainer
}

func getPodSchedulingPolicy(app *marathon.Application) *marathon.PodSchedulingPolicy {
	if app.Constraints == nil {
		return nil
	}
	constraints := make([]marathon.Constraint, len(*app.Constraints))
	for i, con := range *app.Constraints {
		if len(con) >= 2 {
			constraints[i].FieldName = con[0]
			constraints[i].Operator = con[1]
		}
		if len(con) == 3 {
			constraints[i].Value = con[2]
		}
	}
	policy := marathon.NewPodSchedulingPolicy()
	policy.Placement.Constraints = &constraints
	return policy
}

func getPodHealthCheck(app *marathon.Application) (*marathon.PodHealthCheck, error) {
	if app.HealthChecks == nil {
		return nil, nil
	}
	podHealthCheck := &marathon.PodHealthCheck{}
	for _, healthCheck := range *app.HealthChecks {
		if healthCheck.Protocol == "COMMAND" {
			podHealthCheck.SetExecHealthCheck(&marathon.CommandHealthCheck{
				Command: marathon.PodCommand{
					Shell: healthCheck.Command.Value,
				},
			})
			return podHealthCheck, nil
		} else if healthCheck.Protocol == "TCP" {
			if *healthCheck.PortIndex >= len(*app.Container.PortMappings) {
				return nil, fmt.Errorf("application's healthCheck PortIndex is illegal: %v", healthCheck)
			}
			ep := (*app.Container.PortMappings)[*healthCheck.PortIndex]
			podHealthCheck.TCP = &marathon.TCPHealthCheck{
				Endpoint: ep.Name,
			}
			return podHealthCheck, nil
		} else if healthCheck.Protocol == "HTTP" ||  healthCheck.Protocol == "HTTPS" {
			if *healthCheck.PortIndex >= len(*app.Container.PortMappings) {
				return nil, fmt.Errorf("application's healthCheck PortIndex is illegal: %v", healthCheck)
			}
			ep := (*app.Container.PortMappings)[*healthCheck.PortIndex]
			podHealthCheck.HTTP = &marathon.HTTPHealthCheck{
				Endpoint: ep.Name,
				Scheme: healthCheck.Protocol,
				Path: *healthCheck.Path,
			}
			return podHealthCheck, nil
		}
	}
	return nil, nil
}

func validateMesosFlags() error {
	var err error
	if inMesosFilename != "" && emitTemplate {
		err = multierr.Append(err, errors.New("--filename and --emitTemplate are mutually exclusive"))
	}
	if inMesosFilename == "" && !emitTemplate {
		err = multierr.Append(err, errors.New("filename not specified (see --filename or -f)"))
	}

	return err
}


var (
	inMesosFilename          string
	outMesosFilename         string
	
)


func init() {
	rootCmd.AddCommand(injectMesosCmd)

	injectMesosCmd.PersistentFlags().StringVar(&meshConfigFile, "meshConfigFile", "",
		"mesh configuration filename. Takes precedence over --meshConfigMapName if set")
	injectMesosCmd.PersistentFlags().StringVar(&injectConfigFile, "injectConfigFile", "",
		"injection configuration filename. Cannot be used with --injectConfigMapName")
	injectMesosCmd.PersistentFlags().StringVar(&valuesFile, "valuesFile", "",
		"injection values configuration filename.")

	injectMesosCmd.PersistentFlags().BoolVar(&emitTemplate, "emitTemplate", false,
		"Emit sidecar template based on parameterized flags")
	_ = injectMesosCmd.PersistentFlags().MarkHidden("emitTemplate")

	injectMesosCmd.PersistentFlags().StringVarP(&inMesosFilename, "filename", "f",
		"", "Input Kubernetes resource filename")
	injectMesosCmd.PersistentFlags().StringVarP(&outMesosFilename, "output", "o",
		"", "Modified output Kubernetes resource filename")

	injectMesosCmd.PersistentFlags().StringVar(&meshConfigMapName, "meshConfigMapName", defaultMeshConfigMapName,
		fmt.Sprintf("ConfigMap name for Istio mesh configuration, key should be %q", configMapKey))
	injectMesosCmd.PersistentFlags().StringVar(&injectConfigMapName, "injectConfigMapName", defaultInjectConfigMapName,
		fmt.Sprintf("ConfigMap name for Istio sidecar injection, key should be %q.", injectConfigMapKey))
}
