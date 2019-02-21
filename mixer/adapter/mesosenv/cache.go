package mesosenv

import (
	"github.com/harryge00/go-marathon"
	"istio.io/istio/mixer/pkg/adapter"
	"time"
)

type (
	// internal interface used to support testing
	cacheController interface {
		Run(<-chan struct{})

		HasSynced() bool
	}

	controllerImpl struct {
		env           adapter.Env
		depInfoChan      marathon.EventsChannel
		statusUpdateChan marathon.EventsChannel
	}
)

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
	return &controllerImpl{
		env: env,
		depInfoChan: depInfoChan,
		statusUpdateChan: statusUpdateChan,
	}, nil
}

func (c *controllerImpl) Run(stop <-chan struct{}) {
	return
}

func (c *controllerImpl) HasSynced() bool {
	return true
}