package com.example.flink.http;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.HttpSinkBuilder;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.SinkRequestSubmitMode;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static com.example.flink.http.RecordingMetricReporter.counter;
import static com.example.flink.http.RecordingMetricReporter.hasCounter;
import static com.example.flink.http.RecordingMetricReporter.histogram;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Sink observability (FLINK-40281): the metrics of the {@code http_sink_connector} group are read
 * back from a recording metric reporter after each job. Counters count request entries; the batch
 * tests show the difference to {@code numHttpRequests}, which counts HTTP requests.
 */
class DataStreamHttpSinkMetricsIT {

    private static final String GROUP = "http_sink_connector";

    @BeforeEach
    void resetMetrics() {
        RecordingMetricReporter.reset();
    }

    @Test
    void countsRequestsRetriesAndStatusCodesForABatchThatSucceedsOnRetry() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/metrics-batch-retry", 503, 200);

            runJob(
                    server.baseUrl() + "/metrics-batch-retry",
                    List.of("{\"id\":1}", "{\"id\":2}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE,
                            SinkRequestSubmitMode.BATCH.getMode(),
                            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                            "10",
                            HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                            "2",
                            HttpConnectorConfigConstants.SINK_RETRY_CODES,
                            "503",
                            HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY,
                            "10ms"));

            // two HTTP requests (first attempt and retry), each carrying both entries
            assertThat(counter(GROUP + ".numHttpRequests")).isEqualTo(2);
            assertThat(counter(GROUP + ".numRetryableResponseFailures")).isEqualTo(2);
            assertThat(counter(GROUP + ".numRetryAttempts")).isEqualTo(2);
            assertThat(counter(GROUP + ".numRetryExhausted")).isZero();
            assertThat(counter(GROUP + ".numFatalResponseFailures")).isZero();
            assertThat(counter(GROUP + ".status_code.503")).isEqualTo(2);
            assertThat(counter(GROUP + ".status_code.200")).isEqualTo(2);
            assertThat(histogram(GROUP + ".requestLatencyMs"))
                    .hasValueSatisfying(h -> assertThat(h.getCount()).isEqualTo(2));
            assertThat(counter("numRecordsSendErrors")).isZero();
        }
    }

    @Test
    void countsFatalResponsesAndStandardSendErrors() {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/metrics-fatal", 400);

            assertThatThrownBy(
                            () ->
                                    runJob(
                                            server.baseUrl() + "/metrics-fatal",
                                            List.of("{\"id\":1}"),
                                            Map.of(
                                                    HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                                                    "2")))
                    .hasStackTraceContaining("HTTP sink received fatal response status");

            assertThat(counter(GROUP + ".numHttpRequests")).isEqualTo(1);
            assertThat(counter(GROUP + ".numFatalResponseFailures")).isEqualTo(1);
            assertThat(counter(GROUP + ".numRetryableResponseFailures")).isZero();
            assertThat(counter(GROUP + ".status_code.400")).isEqualTo(1);
            assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        }
    }

    @Test
    void countsIgnoredResponsesWithoutErrors() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/metrics-ignored", 404);

            runJob(
                    server.baseUrl() + "/metrics-ignored",
                    List.of("{\"id\":1}"),
                    Map.of(HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES, "404"));

            assertThat(counter(GROUP + ".numIgnoredResponses")).isEqualTo(1);
            assertThat(counter(GROUP + ".status_code.404")).isEqualTo(1);
            assertThat(counter(GROUP + ".numFatalResponseFailures")).isZero();
            assertThat(counter("numRecordsSendErrors")).isZero();
        }
    }

    @Test
    void countsEveryAttemptAndTheExhaustionWhenRetriesRunOut() {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/metrics-exhausted", 500);

            assertThatThrownBy(
                            () ->
                                    runJob(
                                            server.baseUrl() + "/metrics-exhausted",
                                            List.of("{\"id\":1}"),
                                            Map.of(
                                                    HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                                                    "1",
                                                    HttpConnectorConfigConstants.SINK_RETRY_CODES,
                                                    "500",
                                                    HttpConnectorConfigConstants
                                                            .SINK_RETRY_FIXED_DELAY_DELAY,
                                                    "10ms")))
                    .hasStackTraceContaining("HTTP sink exhausted retries");

            // two attempts: the first was retried, the second was the last one allowed
            assertThat(counter(GROUP + ".numHttpRequests")).isEqualTo(2);
            assertThat(counter(GROUP + ".numRetryableResponseFailures")).isEqualTo(2);
            assertThat(counter(GROUP + ".numRetryAttempts")).isEqualTo(1);
            assertThat(counter(GROUP + ".numRetryExhausted")).isEqualTo(1);
            assertThat(counter(GROUP + ".status_code.500")).isEqualTo(2);
            assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
        }
    }

    @Test
    void countsStatusCodesPerRequestEntryOfABatch() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/metrics-weighted", 503, 200);

            runJob(
                    server.baseUrl() + "/metrics-weighted",
                    List.of("{\"id\":1}", "{\"id\":2}", "{\"id\":3}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE,
                            SinkRequestSubmitMode.BATCH.getMode(),
                            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                            "10",
                            HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                            "1",
                            HttpConnectorConfigConstants.SINK_RETRY_CODES,
                            "503",
                            HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY,
                            "10ms"));

            // one 503 for a request with three entries counts three, as the docs describe
            assertThat(counter(GROUP + ".numHttpRequests")).isEqualTo(2);
            assertThat(counter(GROUP + ".status_code.503")).isEqualTo(3);
            assertThat(counter(GROUP + ".status_code.200")).isEqualTo(3);
            assertThat(counter(GROUP + ".numRetryAttempts")).isEqualTo(3);
        }
    }

    @Test
    void countsRequestExceptionsWhenTheEndpointIsUnreachable() throws Exception {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }

        assertThatThrownBy(
                        () ->
                                runJob(
                                        "http://localhost:" + closedPort + "/nobody-listens",
                                        List.of("{\"id\":1}"),
                                        Map.of(HttpConnectorConfigConstants.SINK_MAX_RETRIES, "0")))
                .hasStackTraceContaining("HTTP sink exhausted retries");

        // the request failed before any response, so there is no status code to count
        assertThat(counter(GROUP + ".numHttpRequests")).isEqualTo(1);
        assertThat(counter(GROUP + ".numRequestExceptions")).isEqualTo(1);
        assertThat(counter(GROUP + ".numRetryExhausted")).isEqualTo(1);
        assertThat(hasCounter(GROUP + ".status_code.500")).isFalse();
        assertThat(counter("numRecordsSendErrors")).isEqualTo(1);
    }

    private static void runJob(
            String endpointUrl, List<String> payloads, Map<String, String> properties)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(
                "metrics.reporter." + RecordingMetricReporter.REPORTER_NAME + ".factory.class",
                RecordingMetricReporter.Factory.class.getName());
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(1, configuration);

        HttpSinkBuilder<String> builder =
                HttpSink.<String>builder()
                        .setEndpointUrl(endpointUrl)
                        .setElementConverter(
                                (event, context) ->
                                        new HttpSinkRequestEntry(
                                                "POST", event.getBytes(StandardCharsets.UTF_8)))
                        .setProperty(
                                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Content-Type",
                                "application/json");
        properties.forEach(builder::setProperty);

        env.fromCollection(payloads).sinkTo(builder.build());
        env.execute("DataStream HTTP sink metrics scenario");
    }
}
