package com.example.flink.http;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric reporter for the integration tests. The mini-cluster registers every metric with it, and
 * the tests read sink counters back by the tail of their identifier, for example {@code
 * http_sink_connector.numHttpRequests}. Metrics stay readable after the job has finished because
 * the metric objects are kept, not sampled.
 */
public final class RecordingMetricReporter implements MetricReporter {

    static final String REPORTER_NAME = "recording";

    private static final Map<String, Metric> METRICS = new ConcurrentHashMap<>();

    static void reset() {
        METRICS.clear();
    }

    /** Value of the single counter whose identifier ends with {@code suffix}, or 0 if absent. */
    static long counter(String suffix) {
        return find(suffix).map(metric -> ((Counter) metric).getCount()).orElse(0L);
    }

    static boolean hasCounter(String suffix) {
        return find(suffix).isPresent();
    }

    static Optional<Histogram> histogram(String suffix) {
        return find(suffix).map(Histogram.class::cast);
    }

    private static Optional<Metric> find(String suffix) {
        return METRICS.entrySet().stream()
                .filter(entry -> entry.getKey().endsWith("." + suffix))
                .map(Map.Entry::getValue)
                .findFirst();
    }

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {}

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        METRICS.put(group.getMetricIdentifier(metricName), metric);
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        // keep the metric so tests can read final values after the job finished
    }

    /** Factory the mini-cluster loads through {@code META-INF/services}. */
    public static final class Factory implements MetricReporterFactory {
        @Override
        public MetricReporter createMetricReporter(Properties properties) {
            return new RecordingMetricReporter();
        }
    }
}
