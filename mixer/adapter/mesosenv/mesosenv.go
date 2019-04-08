// Copyright 2017 Istio Authors.
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

// nolint: lll
//go:generate $GOPATH/src/istio.io/istio/bin/mixer_codegen.sh -a mixer/adapter/mesosenv/config/config.proto -x "-n mesosenv"
//go:generate $GOPATH/src/istio.io/istio/bin/mixer_codegen.sh -t mixer/adapter/mesosenv/template/template.proto

// Package mesosenv provides functionality to adapt mixer behavior to the
// mesos environment. Primarily, it is used to generate values as part
// of Mixer's attribute generation preprocessing phase. These values will be
// transformed into attributes that can be used for subsequent config
// resolution and adapter dispatch and execution.
package mesosenv

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/harryge00/go-marathon"
	"istio.io/istio/mixer/adapter/mesosenv/config"
	mtmpl "istio.io/istio/mixer/adapter/mesosenv/template"
	"istio.io/istio/mixer/pkg/adapter"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // needed for auth
)

const (
	defaultMarathonAddress = "http://master.mesos:8080"
	kubePrefix             = "kubernetes://"

	// mesos cache invalidation
	// TODO: determine a reasonable default
	defaultRefreshPeriod = 5 * time.Minute
)

type (
	builder struct {
		adapterConfig *config.Params

		sync.Mutex
		controllers map[string]cacheController
	}

	handler struct {
		mesosCache cacheController
		env        adapter.Env
		params     *config.Params
	}
)

// compile-time validation
var _ mtmpl.HandlerBuilder = &builder{}
var _ mtmpl.Handler = &handler{}

// GetInfo returns the Info associated with this adapter implementation.
func GetInfo() adapter.Info {
	return adapter.Info{
		Name:        "mesosenv",
		Impl:        "istio.io/istio/mixer/adapter/mesosenv",
		Description: "Provides platform specific functionality for the mesos environment",
		SupportedTemplates: []string{
			mtmpl.TemplateName,
		},
		DefaultConfig: &config.Params{
			MarathonAddress:      defaultMarathonAddress,
			CacheRefreshDuration: defaultRefreshPeriod,
		},

		NewBuilder: func() adapter.HandlerBuilder {
			return &builder{
				controllers: make(map[string]cacheController),
			}
		},
	}
}

func (b *builder) SetAdapterConfig(c adapter.Config) {
	b.adapterConfig = c.(*config.Params)
}

// Validate is responsible for ensuring that all the configuration state given to the builder is
// correct.
func (b *builder) Validate() (ce *adapter.ConfigErrors) {
	return
}

func (b *builder) Build(ctx context.Context, env adapter.Env) (adapter.Handler, error) {
	paramsProto := b.adapterConfig
	stopChan := make(chan struct{})
	refresh := paramsProto.CacheRefreshDuration
	env.Logger().Infof("Building marathon handler %v", paramsProto)

	// only ever build a controller for a config once. this potential blocks
	// the Build() for multiple handlers using the same config until the first
	// one has synced. This should be OK, as the WaitForCacheSync was meant to
	// provide this basic functionality before.
	b.Lock()
	defer b.Unlock()
	controller, found := b.controllers[paramsProto.MarathonAddress]
	if !found {
		config := marathon.NewDefaultConfig()
		config.URL = paramsProto.MarathonAddress
		config.EventsTransport = marathon.EventsTransportSSE
		if paramsProto.HttpBasicAuthUser != "" {
			config.HTTPBasicAuthUser = paramsProto.HttpBasicAuthUser
			config.HTTPBasicPassword = paramsProto.HttpBasicAuthPassword
		}
		if !strings.HasPrefix(config.URL, "http") {
			env.Logger().Errorf("marathon address does not specify http protocol. Use http://")
			config.URL = "http://" + config.URL
		}
		env.Logger().Infof("Creating a client, Marathon Config: %s", config)

		client, err := marathon.NewClient(config)
		if err != nil {
			return nil, err
		}

		controller, err = newCacheController(client, refresh, env)
		env.ScheduleDaemon(func() { controller.Run(stopChan) })
		// ensure that any request is only handled after
		// a sync has occurred

		env.Logger().Infof("Syncing with marathon")
		b.controllers[paramsProto.MarathonAddress] = controller
	}

	return &handler{
		env:        env,
		mesosCache: controller,
		params:     paramsProto,
	}, nil
}

func keyFromUID(uid string) (podKey string, taskKey string) {
	taskKey = strings.TrimPrefix(uid, kubePrefix)
	if strings.Contains(taskKey, ".") {
		parts := strings.Split(taskKey, ".")
		if len(parts) == 2 {
			podKey = parts[0]
		}
	}
	return
}

func (h *handler) GenerateMesosAttributes(ctx context.Context, inst *mtmpl.Instance) (*mtmpl.Output, error) {
	out := mtmpl.NewOutput()

	if inst.DestinationUid != "" && inst.DestinationUid != "unknown" {
		if task, found := h.findTask(inst.DestinationUid, inst.DestinationPort); found {
			h.fillDestinationAttrs(task, inst.DestinationPort, out, h.params)
		}
	} else {
		h.env.Logger().Debugf("unknown uid: %v", inst)
	}

	if inst.SourceUid != "" && inst.SourceUid != "unknown" {
		if task, found := h.findTask(inst.SourceUid, 0); found {
			h.fillSourceAttrs(task, out, h.params)
		}
	} else {
		h.env.Logger().Debugf("unknown uid: %v", inst)
	}

	return out, nil
}

func (h *handler) findTask(uid string, port int64) (*TaskInfo, bool) {
	return h.mesosCache.PodTask(uid, int(port))
}

func (h *handler) fillDestinationAttrs(task *TaskInfo, port int64, o *mtmpl.Output, params *config.Params) {
	o.SetDestinationLabels(task.Labels)
	o.SetDestinationHostIp(task.HostIP)
	o.SetDestinationPodIp(task.ContainerIP)
	o.SetDestinationContainerName(task.ContainerName)
	o.SetDestinationPodName(task.PodName)
	o.SetDestinationWorkloadNamespace("default")
	o.SetDestinationNamespace("default")
}

func (h *handler) fillSourceAttrs(task *TaskInfo, o *mtmpl.Output, params *config.Params) {
	o.SetSourceLabels(task.Labels)
	o.SetSourceHostIp(task.HostIP)
	o.SetSourcePodName(task.PodName)
	o.SetSourcePodIp(task.ContainerIP)
	o.SetSourcePodName(task.ContainerName)
}

func (h *handler) Close() error {
	return nil
}
