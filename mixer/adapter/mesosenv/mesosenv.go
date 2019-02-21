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
	"errors"
	"sync"
	"time"

	"istio.io/istio/mixer/adapter/mesosenv/config"
	mtmpl "istio.io/istio/mixer/adapter/mesosenv/template"
	"istio.io/istio/mixer/pkg/adapter"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // needed for auth
	"k8s.io/client-go/tools/cache"

	"github.com/harryge00/go-marathon"
)

const (
	defaultMarathonAddress = "http://master.mesos:8080"

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
		k8sCache cacheController
		env      adapter.Env
		params   *config.Params
	}
)

// compile-time validation
var _ mtmpl.HandlerBuilder = &builder{}

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
			MarathonAddress:       defaultMarathonAddress,
			CacheRefreshDuration: defaultRefreshPeriod,
		},

		NewBuilder: func() adapter.HandlerBuilder { return &builder{} },
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
	env.Logger().Infof("Building marathon handler")

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
		env.Logger().Infof("Creating a client, Marathon Config: %s", config)

		client, err := marathon.NewClient(config)
		if err != nil {
			return nil, err
		}

		controller, err = newCacheController(client, refresh, env)
		env.ScheduleDaemon(func() { controller.Run(stopChan) })
		// ensure that any request is only handled after
		// a sync has occurred
		env.Logger().Infof("Waiting for mesos cache sync...")
		if success := cache.WaitForCacheSync(stopChan, controller.HasSynced); !success {
			stopChan <- struct{}{}
			return nil, errors.New("cache sync failure")
		}
		env.Logger().Infof("Cache sync successful.")
		b.controllers[paramsProto.MarathonAddress] = controller
	}

	return &handler{
		env:      env,
		k8sCache: controller,
		params:   paramsProto,
	}, nil
}

func (h *handler) GeneratemesosAttributes(ctx context.Context, inst *mtmpl.Instance) (*mtmpl.Output, error) {
	out := mtmpl.NewOutput()
	h.env.Logger().Infof("GeneratemesosAttributes %v ", inst.Name)

	out.SetDestinationPodName("testpod")
	out.SetDestinationPodName("testns")
	return out, nil
}

func (h *handler) Close() error {
	return nil
}
