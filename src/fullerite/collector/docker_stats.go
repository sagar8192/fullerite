package collector

import (
	"fullerite/config"
	"fullerite/metric"
	"math"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	l "github.com/Sirupsen/logrus"

	"github.com/fsouza/go-dockerclient"
)

const (
	endpoint = "unix:///var/run/docker.sock"
)

type rawStats struct {
	Container *docker.Container
	Stats     *docker.Stats
}

type statsStore struct {
	statsPerImage map[string][]*rawStats
	lock          *sync.Mutex
	lastFlush     int32
}

// DockerStats collector type.
// previousCPUValues contains the last cpu-usage values per container.
// dockerClient is the client for the Docker remote API.
type DockerStats struct {
	baseCollector
	previousCPUValues map[string]*CPUValues
	dockerClient      *docker.Client
	statsTimeout      int
	compiledRegex     map[string]*Regex
	skipRegex         *regexp.Regexp
	endpoint          string
	mu                *sync.Mutex
	emitImageName     bool
	aggregateMetrics  bool
	statsStore        *statsStore
}

// CPUValues struct contains the last cpu-usage values in order to compute properly the current values.
// (see calculateCPUPercent() for more details)
type CPUValues struct {
	totCPU, systemCPU uint64
}

// Regex struct contains the info used to get the user specific dimensions from the docker env variables
// tag: is the environmental variable you want to get the value from
// regex: is the reg exp used to extract the value from the env var
type Regex struct {
	tag   string
	regex *regexp.Regexp
}

func init() {
	RegisterCollector("DockerStats", newDockerStats)
}

// newDockerStats creates a new DockerStats collector.
func newDockerStats(channel chan metric.Metric, initialInterval int, log *l.Entry) Collector {
	d := new(DockerStats)

	d.log = log
	d.channel = channel
	d.interval = initialInterval
	d.mu = new(sync.Mutex)

	d.name = "DockerStats"
	d.previousCPUValues = make(map[string]*CPUValues)
	d.compiledRegex = make(map[string]*Regex)
	d.emitImageName = false
	d.aggregateMetrics = false
	d.statsStore = &statsStore{
		statsPerImage: make(map[string][]*rawStats),
		lock:          new(sync.Mutex),
		lastFlush:     int32(time.Now().Unix()),
	}
	return d
}

// GetEndpoint Returns endpoint of DockerStats instance
func (d *DockerStats) GetEndpoint() string {
	return d.endpoint
}

// Configure takes a dictionary of values with which the handler can configure itself.
func (d *DockerStats) Configure(configMap map[string]interface{}) {
	if timeout, exists := configMap["dockerStatsTimeout"]; exists {
		d.statsTimeout = min(config.GetAsInt(timeout, d.interval), d.interval)
	} else {
		d.statsTimeout = d.interval
	}
	if dockerEndpoint, exists := configMap["dockerEndPoint"]; exists {
		if str, ok := dockerEndpoint.(string); ok {
			d.endpoint = str
		} else {
			d.log.Warn("Failed to cast dokerEndPoint: ", reflect.TypeOf(dockerEndpoint))
		}
	} else {
		d.endpoint = endpoint
	}
	if emitImageName, exists := configMap["emit_image_name"]; exists {
		if boolean, ok := emitImageName.(bool); ok {
			d.emitImageName = boolean
		} else {
			d.log.Warn("Failed to cast emit_image_name: ", reflect.TypeOf(emitImageName))
		}
	}

    if aggregateMetrics, exists := configMap["aggregate_metrics"]; exists {
		if boolean, ok := aggregateMetrics.(bool); ok {
			d.aggregateMetrics = boolean
		} else {
			d.log.Warn("Failed to cast aggregate_metrics: ", reflect.TypeOf(aggregateMetrics))
		}
	}

	d.dockerClient, _ = docker.NewClient(d.endpoint)
	if generatedDimensions, exists := configMap["generatedDimensions"]; exists {
		for dimension, generator := range generatedDimensions.(map[string]interface{}) {
			for key, regx := range config.GetAsMap(generator) {
				re, err := regexp.Compile(regx)
				if err != nil {
					d.log.Warn("Failed to compile regex: ", regx, err)
				} else {
					d.compiledRegex[dimension] = &Regex{regex: re, tag: key}
				}
			}
		}
	}
	d.configureCommonParams(configMap)
	if skipRegex, skipExists := configMap["skipContainerRegex"]; skipExists {
		d.skipRegex = regexp.MustCompile(skipRegex.(string))
	}
}

// Collect iterates on all the docker containers alive and, if possible, collects the correspondent
// memory and cpu statistics.
// For each container a gorutine is started to spin up the collection process.
func (d *DockerStats) Collect() {
	if d.dockerClient == nil {
		d.log.Error("Invalid endpoint: ", docker.ErrInvalidEndpoint)
		return
	}
	d.flushMetricsIfNecessary()
	containers, err := d.dockerClient.ListContainers(docker.ListContainersOptions{All: false})
	if err != nil {
		d.log.Error("ListContainers() failed: ", err)
		return
	}
	for _, apiContainer := range containers {
		container, err := d.dockerClient.InspectContainer(apiContainer.ID)

		if err != nil {
			d.log.Error("InspectContainer() failed: ", err)
			continue
		}

		if d.skipRegex != nil && d.skipRegex.MatchString(container.Name) {
			d.log.Info("Skip container: ", container.Name)
			continue
		}
		if _, ok := d.previousCPUValues[container.ID]; !ok {
			d.previousCPUValues[container.ID] = new(CPUValues)
		}
		go d.getDockerContainerInfo(container)
	}
}

// getDockerContainerInfo gets container statistics for the given container.
// results is a channel to make possible the synchronization between the main process and the gorutines (wait-notify pattern).
func (d *DockerStats) getDockerContainerInfo(container *docker.Container) {
	errC := make(chan error, 1)
	statsC := make(chan *docker.Stats, 1)
	done := make(chan bool, 1)

	go func() {
		errC <- d.dockerClient.Stats(docker.StatsOptions{container.ID, statsC, false, done, time.Second * time.Duration(d.interval)})
	}()
	select {
	case stats, ok := <-statsC:
		if !ok {
			err := <-errC
			d.log.Error("Failed to collect docker container stats: ", err)
			break
		}
		done <- true

		if d.aggregateMetrics {
			d.bufferMetrics(container, stats)
		} else {
			metrics := d.extractMetrics(container, stats)
			d.sendMetrics(metrics)
		}

		break
	case <-time.After(time.Duration(d.statsTimeout) * time.Second):
		d.log.Error("Timed out collecting stats for container ", container.ID)
		done <- true
		break
	}
}

func (d *DockerStats) flushMetricsIfNecessary() {
	if d.aggregateMetrics {
		if (int32(time.Now().Unix()) - d.statsStore.lastFlush) > 60 {
			// Aggregate and flush metrics
			d.aggregateAndEmitMetrics()
		}
	}
}

func (d *DockerStats) aggregateAndEmitMetrics() {
	d.statsStore.lock.Lock()
	d.statsStore.lock.Unlock()

	metrics := []metric.Metric{}
	for key, value := range d.statsStore.statsPerImage {
		maxMem := float64(0.0)
		for _, rawstats := range value {
			maxMem = math.Max(float64(rawstats.Stats.MemoryStats.Usage), maxMem)
		}
		metric := buildDockerMetric("DockerMemoryUsed", metric.Gauge, maxMem)
		metric.AddDimension("image_name", key)
		metric.AddDimension("roll_up", "max")
		metrics = append(metrics, metric)
	}
	d.log.Warn("Going to flush metrics, count :", len(metrics))
	d.sendMetrics(metrics)
	d.statsStore.lastFlush = int32(time.Now().Unix())
}

func (d *DockerStats) bufferMetrics(container *docker.Container, stats *docker.Stats) {
	d.statsStore.lock.Lock()
	d.statsStore.lock.Unlock()

	rawstats := rawStats{
		Container: container,
		Stats:     stats,
	}

	stringList := strings.Split(container.Config.Image, ":")
	imageName := stringList[0]
	if stats, ok := d.statsStore.statsPerImage[imageName]; ok {
		stats = append(stats, &rawstats)
	} else {
		d.log.Warn("Going add the first entry to map!!!")
		d.statsStore.statsPerImage[imageName] = []*rawStats{}
		d.statsStore.statsPerImage[imageName] = append(
			d.statsStore.statsPerImage[imageName],
			&rawstats,
		)
	}
}

func (d *DockerStats) extractMetrics(container *docker.Container, stats *docker.Stats) []metric.Metric {
	d.mu.Lock()
	defer d.mu.Unlock()
	metrics := d.buildMetrics(container, stats, calculateCPUPercent(d.previousCPUValues[container.ID].totCPU, d.previousCPUValues[container.ID].systemCPU, stats))

	d.previousCPUValues[container.ID].totCPU = stats.CPUStats.CPUUsage.TotalUsage
	d.previousCPUValues[container.ID].systemCPU = stats.CPUStats.SystemCPUUsage
	return metrics
}

// buildMetrics creates the actual metrics for the given container.
func (d DockerStats) buildMetrics(container *docker.Container, containerStats *docker.Stats, cpuPercentage float64) []metric.Metric {
	ret := []metric.Metric{
		buildDockerMetric("DockerMemoryUsed", metric.Gauge, float64(containerStats.MemoryStats.Usage)),
		buildDockerMetric("DockerMemoryLimit", metric.Gauge, float64(containerStats.MemoryStats.Limit)),
		buildDockerMetric("DockerCpuPercentage", metric.Gauge, cpuPercentage),
		buildDockerMetric("DockerCpuThrottledPeriods", metric.CumulativeCounter, float64(containerStats.CPUStats.ThrottlingData.ThrottledPeriods)),
		buildDockerMetric("DockerCpuThrottledNanoseconds", metric.CumulativeCounter, float64(containerStats.CPUStats.ThrottlingData.ThrottledTime)),
	}
	for netiface := range containerStats.Networks {
		// legacy format
		txb := buildDockerMetric("DockerTxBytes", metric.CumulativeCounter, float64(containerStats.Networks[netiface].TxBytes))
		txb.AddDimension("iface", netiface)
		ret = append(ret, txb)
		rxb := buildDockerMetric("DockerRxBytes", metric.CumulativeCounter, float64(containerStats.Networks[netiface].RxBytes))
		rxb.AddDimension("iface", netiface)
		ret = append(ret, rxb)
	}
	additionalDimensions := map[string]string{}
	if d.emitImageName {
		stringList := strings.Split(container.Config.Image, ":")
		additionalDimensions = map[string]string{
			"image_name": stringList[0],
		}
	} else {
		additionalDimensions = map[string]string{
			"container_id":   container.ID,
			"container_name": strings.TrimPrefix(container.Name, "/"),
		}
	}
	metric.AddToAll(&ret, additionalDimensions)
	ret = append(ret, buildDockerMetric("DockerContainerCount", metric.Counter, 1))
	metric.AddToAll(&ret, d.extractDimensions(container))
	return ret
}

// sendMetrics writes all the metrics received to the collector channel.
func (d DockerStats) sendMetrics(metrics []metric.Metric) {
	d.log.Warn("Sending metrics!!!")
	for _, m := range metrics {
		d.Channel() <- m
	}
}

// Function that extracts additional dimensions from the docker environmental variables set up by the user
// in the configuration file.
func (d DockerStats) extractDimensions(container *docker.Container) map[string]string {
	envVars := container.Config.Env
	ret := map[string]string{}

	for dimension, r := range d.compiledRegex {
		for _, envVariable := range envVars {
			envArray := strings.Split(envVariable, "=")
			if r.tag == envArray[0] {
				subMatch := r.regex.FindStringSubmatch(envArray[1])
				if len(subMatch) > 0 {
					ret[dimension] = strings.Replace(subMatch[len(subMatch)-1], "--", "_", -1)
				}
			}
		}
	}
	d.log.Debug(ret)
	return ret
}

func buildDockerMetric(name string, metricType string, value float64) (m metric.Metric) {
	m = metric.New(name)
	m.MetricType = metricType
	m.Value = value
	return m
}

// Function that compute the current cpu usage percentage combining current and last values.
func calculateCPUPercent(previousCPU, previousSystem uint64, stats *docker.Stats) float64 {
	var (
		cpuPercent = 0.0
		// calculate the change for the cpu usage of the container in between readings
		cpuDelta = float64(stats.CPUStats.CPUUsage.TotalUsage - previousCPU)
		// calculate the change for the entire system between readings
		systemDelta = float64(stats.CPUStats.SystemCPUUsage - previousSystem)
	)

	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * float64(len(stats.CPUStats.CPUUsage.PercpuUsage)) * 100.0
	}
	return cpuPercent
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
