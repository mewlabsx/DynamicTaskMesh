package main

import (
	"fmt"

	"dtm/internal/agent"
	"dtm/internal/capability"
	"dtm/internal/config"
	"dtm/internal/demo"
	"dtm/internal/model"
	"dtm/internal/runtimehost"
)

type dependencies struct {
	*runtimehost.ResourceExecutionCapability
}

func compose(agentConfig config.Agent) (dependencies, error) {
	handlers := make([]agent.Handler, 0, len(agentConfig.Node.Capabilities))
	for _, configured := range agentConfig.Node.Capabilities {
		switch model.Capability(configured) {
		case capability.TemperatureSensor:
			handlers = append(handlers, demo.NewTemperatureHandler())
		case capability.CoolingControl:
			handlers = append(handlers, demo.NewCoolingHandler())
		default:
			return dependencies{}, fmt.Errorf("unsupported agent capability %q", configured)
		}
	}
	wrapped, err := wrapHandlersForProcessTest(handlers)
	if err != nil {
		return dependencies{}, err
	}
	capability, err := runtimehost.NewResourceExecutionCapability(agentConfig, wrapped...)
	if err != nil {
		return dependencies{}, err
	}
	return dependencies{ResourceExecutionCapability: capability}, nil
}
