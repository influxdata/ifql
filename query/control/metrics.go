package control

import "github.com/prometheus/client_golang/prometheus"

var compilingGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "query_control_compiling_active",
	Help: "Number of queries actively compiling",
})
var queueingGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "query_control_queueing_active",
	Help: "Number of queries actively queueing",
})
var requeueingGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "query_control_requeueing_active",
	Help: "Number of queries actively requeueing",
})
var planningGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "query_control_planning_active",
	Help: "Number of queries actively planning",
})
var executingGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "query_control_executing_active",
	Help: "Number of queries actively executing",
})

var compilingHist = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "query_control_compiling_duration_seconds",
	Help:    "Histogram of times spent compiling queries",
	Buckets: prometheus.ExponentialBuckets(1e-3, 5, 7),
})
var queueingHist = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "query_control_queueing_duration_seconds",
	Help:    "Histogram of times spent queueing queries",
	Buckets: prometheus.ExponentialBuckets(1e-3, 5, 7),
})
var requeueingHist = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "query_control_requeueing_duration_seconds",
	Help:    "Histogram of times spent requeueing queries",
	Buckets: prometheus.ExponentialBuckets(1e-3, 5, 7),
})
var planningHist = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "query_control_planning_duration_seconds",
	Help:    "Histogram of times spent planning queries",
	Buckets: prometheus.ExponentialBuckets(1e-5, 5, 7),
})
var executingHist = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "query_control_executing_duration_seconds",
	Help:    "Histogram of times spent executing queries",
	Buckets: prometheus.ExponentialBuckets(1e-3, 5, 7),
})

func init() {
	prometheus.MustRegister(queueingGauge)
	prometheus.MustRegister(requeueingGauge)
	prometheus.MustRegister(planningGauge)
	prometheus.MustRegister(executingGauge)

	prometheus.MustRegister(queueingHist)
	prometheus.MustRegister(requeueingHist)
	prometheus.MustRegister(planningHist)
	prometheus.MustRegister(executingHist)
}
