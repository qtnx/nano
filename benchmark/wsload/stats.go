package main

import (
	"math"
	"sort"
	"sync"
	"time"
)

const maxLatencySamples = 100000

const (
	errorDial             = "dial"
	errorHandshake        = "handshake"
	errorHandshakeTimeout = "handshake_timeout"
	errorKick             = "server_kick"
	errorRead             = "read"
	errorWrite            = "write"
	errorProtocol         = "protocol"
)

type Parameters struct {
	URL               string  `json:"url"`
	Connections       int     `json:"connections"`
	RampMS            float64 `json:"ramp_ms"`
	DurationMS        float64 `json:"duration_ms"`
	RequestRoute      string  `json:"request_route"`
	RequestJSON       string  `json:"request_json"`
	RequestEveryMS    float64 `json:"request_every_ms"`
	ConnectTimeoutMS  float64 `json:"connect_timeout_ms"`
	Output            string  `json:"output"`
	FailOnErrorRate   *float64 `json:"fail_on_error_rate"`
	LatencySampleCap  int     `json:"latency_sample_cap"`
}

type Percentiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}
type RequestLatency struct {
	Count uint64 `json:"count"`
	Percentiles
}

type Result struct {
	Parameters               Parameters         `json:"parameters"`
	Attempted                int                `json:"attempted"`
	Connected                int                `json:"connected"`
	Failed                   int                `json:"failed"`
	PeakActive               int                `json:"peak_active"`
	ActiveAtEnd              int                `json:"active_at_end"`
	UnexpectedDisconnects    int                `json:"unexpected_disconnects"`
	ConnectedClientSeconds   float64            `json:"connected_client_seconds"`
	SetupMS                  Percentiles        `json:"setup_ms"`
	RequestMS                RequestLatency     `json:"request_ms"`
	Errors                   map[string]int     `json:"errors"`
	StartedAt                time.Time          `json:"started_at"`
	EndedAt                  time.Time          `json:"ended_at"`
	DurationMS               float64            `json:"duration_ms"`
}

type sampleSource interface {
	Uint64n(uint64) uint64
}

type xorshift64 struct {
	state uint64
}

func newXorshift64(seed uint64) *xorshift64 {
	if seed == 0 {
		seed = 0x9e3779b97f4a7c15
	}
	return &xorshift64{state: seed}
}

func (x *xorshift64) Uint64n(n uint64) uint64 {
	limit := ^uint64(0) - (^uint64(0) % n)
	for {
		x.state ^= x.state << 13
		x.state ^= x.state >> 7
		x.state ^= x.state << 17
		if x.state < limit {
			return x.state % n
		}
	}
}

type latencySamples struct {
	values []float64
	seen   uint64
}

type stats struct {
	mu sync.Mutex

	cfg     Config
	sampler sampleSource

	attempted             int
	connected             int
	failed                int
	active                int
	peakActive            int
	unexpectedDisconnects int
	connectedClientNanos  int64
	errors                map[string]int
	setup                 latencySamples
	request               latencySamples
}

func newStats(cfg Config) *stats {
	return newStatsWithSource(cfg, newXorshift64(0x9e3779b97f4a7c15))
}

func newStatsWithSource(cfg Config, sampler sampleSource) *stats {
	return &stats{cfg: cfg, sampler: sampler, errors: make(map[string]int)}
}

func (s *stats) attempt() {
	s.mu.Lock()
	s.attempted++
	s.mu.Unlock()
}

func (s *stats) connect(setup time.Duration) {
	s.mu.Lock()
	s.connected++
	s.active++
	if s.active > s.peakActive {
		s.peakActive = s.active
	}
	s.addSample(&s.setup, setup)
	s.mu.Unlock()
}

func (s *stats) fail(category string) {
	s.mu.Lock()
	s.failed++
	s.errors[category]++
	s.mu.Unlock()
}

func (s *stats) error(category string) {
	s.mu.Lock()
	s.errors[category]++
	s.mu.Unlock()
}

func (s *stats) disconnect(duration time.Duration, unexpected bool) {
	s.mu.Lock()
	if s.active > 0 {
		s.active--
	}
	s.connectedClientNanos += duration.Nanoseconds()
	if unexpected {
		s.unexpectedDisconnects++
	}
	s.mu.Unlock()
}

func (s *stats) markUnexpectedDisconnect() {
	s.mu.Lock()
	s.unexpectedDisconnects++
	s.mu.Unlock()
}

func (s *stats) addSetup(duration time.Duration) {
	s.mu.Lock()
	s.addSample(&s.setup, duration)
	s.mu.Unlock()
}

func (s *stats) addRequest(duration time.Duration) {
	s.mu.Lock()
	s.addSample(&s.request, duration)
	s.mu.Unlock()
}

// addSample uses Algorithm R reservoir sampling. The source is invoked while
// stats.mu is held, so its deterministic sequence is race-free and injectable
// in tests. The reservoir remains bounded while representing the whole run.
func (s *stats) addSample(samples *latencySamples, duration time.Duration) {
	samples.seen++
	value := float64(duration) / float64(time.Millisecond)
	if len(samples.values) < maxLatencySamples {
		samples.values = append(samples.values, value)
		return
	}
	if index := s.sampler.Uint64n(samples.seen); index < maxLatencySamples {
		samples.values[index] = value
	}
}

func (s *stats) result(started, ended time.Time) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	setup := append([]float64(nil), s.setup.values...)
	request := append([]float64(nil), s.request.values...)
	sort.Float64s(setup)
	sort.Float64s(request)
	errors := make(map[string]int, len(s.errors))
	for key, value := range s.errors {
		errors[key] = value
	}
	return Result{
		Parameters:             parameters(s.cfg),
		Attempted:              s.attempted,
		Connected:              s.connected,
		Failed:                 s.failed,
		PeakActive:             s.peakActive,
		ActiveAtEnd:            s.active,
		UnexpectedDisconnects:  s.unexpectedDisconnects,
		ConnectedClientSeconds: float64(s.connectedClientNanos) / float64(time.Second),
		SetupMS:                percentiles(setup),
		RequestMS:              RequestLatency{Count: s.request.seen, Percentiles: percentiles(request)},
		Errors:                 errors,
		StartedAt:              started.UTC(),
		EndedAt:                ended.UTC(),
		DurationMS:             float64(ended.Sub(started)) / float64(time.Millisecond),
	}
}

func parameters(cfg Config) Parameters {
	output := cfg.Output
	if output == "" {
		output = "json"
	}
	return Parameters{
		URL:              cfg.URL,
		Connections:      cfg.Connections,
		RampMS:           float64(cfg.Ramp) / float64(time.Millisecond),
		DurationMS:       float64(cfg.Duration) / float64(time.Millisecond),
		RequestRoute:     cfg.RequestRoute,
		RequestJSON:      string(cfg.RequestJSON),
		RequestEveryMS:   float64(cfg.RequestEvery) / float64(time.Millisecond),
		ConnectTimeoutMS: float64(cfg.ConnectTimeout) / float64(time.Millisecond),
		Output:           output,
		FailOnErrorRate:  cfg.FailOnErrorRate,
		LatencySampleCap: maxLatencySamples,
	}
}

func percentiles(samples []float64) Percentiles {
	return Percentiles{P50: percentile(samples, 0.50), P95: percentile(samples, 0.95), P99: percentile(samples, 0.99)}
}

func percentile(samples []float64, quantile float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	index := int(math.Ceil(quantile*float64(len(samples)))) - 1
	if index < 0 {
		index = 0
	}
	return samples[index]
}
