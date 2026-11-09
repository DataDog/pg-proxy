// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import "time"

// CloudPoolConfig configures a static set of directly addressable PostgreSQL
// servers.
type CloudPoolConfig struct {
	Direct         *DirectRuntime
	Instances      map[string]PostgresInstance
	HealthInterval time.Duration
	SetupFinished  func(time.Duration)
	Start          bool
}

// CloudPool manages a static set of PostgreSQL instances.
//
// TODO: Consider renaming this to StaticDirectPool in a follow-up API RFC.
// Nothing in this runtime is cloud-provider-specific.
type CloudPool struct {
	*DirectRuntime
	healthInterval time.Duration
	setupFinished  func(time.Duration)
}

// NewCloudPool creates and initializes a static direct pool.
func NewCloudPool(cfg CloudPoolConfig) *CloudPool {
	runtime := &CloudPool{
		DirectRuntime:  cfg.Direct,
		healthInterval: cfg.HealthInterval,
		setupFinished:  cfg.SetupFinished,
	}
	for address, instance := range cfg.Instances {
		runtime.State().Store(address, instance)
	}
	runtime.Setup()
	if cfg.Start {
		go runtime.run()
	}
	return runtime
}

// Setup refreshes health and selection state.
func (p *CloudPool) Setup() {
	start := time.Now()
	p.CheckHealth()
	p.RefreshHealthy()
	if p.setupFinished != nil {
		p.setupFinished(time.Since(start))
	}
}

func (p *CloudPool) run() {
	ticker := time.NewTicker(p.healthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.Setup()
		case <-p.Done():
			return
		}
	}
}
