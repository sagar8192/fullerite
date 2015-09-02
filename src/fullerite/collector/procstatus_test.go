package collector_test

import (
	"fullerite/collector"

	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProcStatusConfigureEmptyConfig(t *testing.T) {
	config := make(map[string]interface{})
	ps := collector.NewProcStatus()
	ps.Configure(config)

	assert.Equal(t,
		ps.Interval(),
		collector.DefaultCollectionInterval,
		"should be the default collection interval",
	)
	assert.Equal(t,
		ps.ProcessName(),
		"",
		"should be the default process name",
	)
}

func TestProcStatusConfigure(t *testing.T) {
	config := make(map[string]interface{})
	config["interval"] = 9999
	config["processName"] = "fullerite"
	ps := collector.NewProcStatus()
	ps.Configure(config)

	assert.Equal(t,
		ps.Interval(),
		9999,
		"should be the defined interval",
	)
	assert.Equal(t,
		ps.ProcessName(),
		"fullerite",
		"should be the defined process name",
	)
}
