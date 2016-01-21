package handler

import (
	"fullerite/metric"
	"fullerite/util"

	"bytes"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
)

// SignalFx Handler
type SignalFx struct {
	BaseHandler
	endpoint   string
	authToken  string
	httpClient *util.HTTPAlive
}

// NewSignalFx returns a new SignalFx handler.
func NewSignalFx(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) *SignalFx {

	inst := new(SignalFx)
	inst.name = "SignalFx"

	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.timeout = initialTimeout
	inst.maxIdleConnectionsPerHost = DefaultMaxIdleConnectionsPerHost
	inst.keepAliveInterval = DefaultKeepAliveInterval
	inst.log = log
	inst.channel = channel

	return inst
}

// Configure accepts the different configuration options for the signalfx handler
func (s *SignalFx) Configure(configMap map[string]interface{}) {
	if authToken, exists := configMap["authToken"]; exists {
		s.authToken = authToken.(string)
	} else {
		s.log.Error("There was no auth key specified for the SignalFx Handler, there won't be any emissions")
	}
	if endpoint, exists := configMap["endpoint"]; exists {
		s.endpoint = endpoint.(string)
	} else {
		s.log.Error("There was no endpoint specified for the SignalFx Handler, there won't be any emissions")
	}

	s.configureCommonParams(configMap)
}

// Endpoint returns SignalFx' API endpoint
func (s *SignalFx) Endpoint() string {
	return s.endpoint
}

// Run runs the handler main loop
func (s *SignalFx) Run() {
	httpAliveClient := new(util.HTTPAlive)
	httpAliveClient.Configure(s.timeout,
		time.Duration(s.KeepAliveInterval())*time.Second,
		s.MaxIdleConnectionsPerHost())
	s.httpClient = httpAliveClient

	s.run(s.emitMetrics)
}

func (s *SignalFx) convertToProto(incomingMetric metric.Metric) *DataPoint {
	// Create a new values for the Datapoint that requires pointers.
	outname := s.Prefix() + incomingMetric.Name
	value := incomingMetric.Value

	now := time.Now().UnixNano() / int64(time.Millisecond)
	datapoint := new(DataPoint)
	datapoint.Timestamp = &now
	datapoint.Metric = &outname
	datapoint.Value = &Datum{
		DoubleValue: &value,
	}
	datapoint.Source = new(string)
	*datapoint.Source = "fullerite"

	switch incomingMetric.MetricType {
	case metric.Gauge:
		datapoint.MetricType = MetricType_GAUGE.Enum()
	case metric.Counter:
		datapoint.MetricType = MetricType_COUNTER.Enum()
	case metric.CumulativeCounter:
		datapoint.MetricType = MetricType_CUMULATIVE_COUNTER.Enum()
	}

	dimensions := incomingMetric.GetDimensions(s.DefaultDimensions())
	for key, value := range dimensions {
		// Dimension (protobuf) require a pointer to string
		// values. We need to create new string objects in the
		// scope of this for loop not to repeatedly add the
		// same key:value pairs to the the datapoint.
		dimensionKey := key
		dimensionValue := value
		dim := Dimension{
			Key:   &dimensionKey,
			Value: &dimensionValue,
		}
		datapoint.Dimensions = append(datapoint.Dimensions, &dim)
	}

	return datapoint
}

func (s *SignalFx) emitMetrics(metrics []metric.Metric) bool {
	s.log.Info("Starting to emit ", len(metrics), " metrics")

	if len(metrics) == 0 {
		s.log.Warn("Skipping send because of an empty payload")
		return false
	}

	datapoints := make([]*DataPoint, 0, len(metrics))
	for _, m := range metrics {
		datapoints = append(datapoints, s.convertToProto(m))
	}

	payload := new(DataPointUploadMessage)
	payload.Datapoints = datapoints

	if s.authToken == "" || s.endpoint == "" {
		s.log.Warn("Skipping emission because we're missing the auth token ",
			"or the endpoint, payload would have been ", payload)
		return false
	}
	serialized, err := proto.Marshal(payload)
	if err != nil {
		s.log.Error("Failed to serailize payload ", payload)
		return false
	}

	customHeader := map[string]string{
		"X-SF-TOKEN":   s.authToken,
		"Content-Type": "application/x-protobuf",
	}

	rsp, err := s.httpClient.MakeRequest(
		"POST",
		s.endpoint,
		bytes.NewBuffer(serialized),
		customHeader)

	if err != nil {
		s.log.Error("Failed to make request ", err,
			" to endpoint ", s.endpoint)
		return false
	}

	if rsp.StatusCode != 200 {
		s.log.Error("Failed to post to signalfx @", s.endpoint,
			" status was ", rsp.StatusCode,
			" rsp body was ", string(rsp.Body),
			" payload was ", payload)
		return false
	}

	s.log.Info("Successfully sent ", len(datapoints), " datapoints to SignalFx")
	return true
}
