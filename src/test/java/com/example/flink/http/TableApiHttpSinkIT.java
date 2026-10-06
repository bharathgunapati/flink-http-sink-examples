package com.example.flink.http;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableApiHttpSinkIT {

    @Test
    void retriesTransientServerErrorInBatchModeFromDdlOptions() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/table-batch-retry", 500, 200);

            runInsert(
                    server.baseUrl() + "/table-batch-retry",
                    Map.of(
                            "http.sink.writer.request.mode",
                            "batch",
                            "http.sink.request.batch.size",
                            "10",
                            "http.sink.max-retries",
                            "2",
                            "http.sink.retry-codes",
                            "500",
                            "http.sink.retry-strategy.fixed-delay.delay",
                            "10ms"));

            assertThat(server.requestCount("/table-batch-retry")).isEqualTo(2);
            assertThat(server.requests("/table-batch-retry"))
                    .extracting(MockHttpServer.RecordedRequest::body)
                    .containsExactly(
                            "[{\"id\":1,\"event_type\":\"table-api\"},{\"id\":2,\"event_type\":\"batch-retry\"}]",
                            "[{\"id\":1,\"event_type\":\"table-api\"},{\"id\":2,\"event_type\":\"batch-retry\"}]");
        }
    }

    @Test
    void supportsCustomSuccessCodesFromDdlOptions() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/accepted", 202);

            runInsert(
                    server.baseUrl() + "/accepted",
                    Map.of(
                            "http.sink.success-codes",
                            "202",
                            "http.sink.retry-codes",
                            "500",
                            "http.sink.max-retries",
                            "2"));

            assertThat(server.requestCount("/accepted")).isEqualTo(1);
        }
    }

    @Test
    void treatsIgnoredStatusAsSuccessfulFromDdlOptions() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/table-ignored", 404);

            runInsert(
                    server.baseUrl() + "/table-ignored",
                    Map.of(
                            "http.sink.ignored-response-codes",
                            "404",
                            "http.sink.max-retries",
                            "2"));

            assertThat(server.requestCount("/table-ignored")).isEqualTo(1);
        }
    }

    @Test
    void failsImmediatelyForFatalStatusThatIsNotRetryable() {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/fatal", 400);

            assertThatThrownBy(
                            () ->
                                    runInsert(
                                            server.baseUrl() + "/fatal",
                                            Map.of(
                                                    "http.sink.retry-codes",
                                                    "500",
                                                    "http.sink.max-retries",
                                                    "2")))
                    .hasStackTraceContaining("HTTP sink received fatal response status");

            assertThat(server.requestCount("/fatal")).isEqualTo(1);
        }
    }

    @Test
    void supportsExponentialDelayRetryOptionsFromDdl() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/table-exponential", 503, 200);

            runInsert(
                    server.baseUrl() + "/table-exponential",
                    Map.of(
                            "http.sink.max-retries",
                            "2",
                            "http.sink.retry-codes",
                            "503",
                            "http.sink.retry-strategy.type",
                            "exponential-delay",
                            "http.sink.retry-strategy.exponential-delay.initial-backoff",
                            "10ms",
                            "http.sink.retry-strategy.exponential-delay.max-backoff",
                            "100ms",
                            "http.sink.retry-strategy.exponential-delay.backoff-multiplier",
                            "2.0"));

            assertThat(server.requestCount("/table-exponential")).isEqualTo(2);
        }
    }

    @Test
    void supportsLegacyErrorCodeOptionsFromDdl() {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/table-legacy-fatal", 500);

            assertThatThrownBy(
                            () ->
                                    runInsert(
                                            server.baseUrl() + "/table-legacy-fatal",
                                            Map.of("http.sink.error.code", "5XX")))
                    .hasStackTraceContaining("HTTP sink received fatal response status");

            assertThat(server.requestCount("/table-legacy-fatal")).isEqualTo(1);
        }
    }

    @Test
    void rejectsMixedLegacyAndNewStatusCodeOptionsFromDdl() {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/table-mixed", 200);

            assertThatThrownBy(
                            () ->
                                    runInsert(
                                            server.baseUrl() + "/table-mixed",
                                            Map.of(
                                                    "http.sink.error.code",
                                                    "4XX",
                                                    "http.sink.success-codes",
                                                    "2XX")))
                    .hasStackTraceContaining(
                            "Cannot set legacy HTTP sink error-code properties");
        }
    }

    private static void runInsert(String endpointUrl, Map<String, String> options) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String tableName = "http_events_" + UUID.randomUUID().toString().replace("-", "");

        tableEnv.executeSql(createTableDdl(tableName, endpointUrl, options));
        tableEnv.executeSql(
                        "INSERT INTO "
                                + tableName
                                + " VALUES "
                                + "(1, 'table-api'), "
                                + "(2, 'batch-retry')")
                .await();
    }

    private static String createTableDdl(
            String tableName, String endpointUrl, Map<String, String> options) {
        StringBuilder ddl =
                new StringBuilder()
                        .append("CREATE TABLE ")
                        .append(tableName)
                        .append(" (\n")
                        .append("  id BIGINT,\n")
                        .append("  event_type STRING\n")
                        .append(") WITH (\n")
                        .append("  'connector' = 'http-async-sink',\n")
                        .append("  'url' = '")
                        .append(endpointUrl)
                        .append("',\n")
                        .append("  'format' = 'json',\n")
                        .append("  'http.sink.header.Content-Type' = 'application/json'");

        options.forEach(
                (key, value) ->
                        ddl.append(",\n")
                                .append("  '")
                                .append(key)
                                .append("' = '")
                                .append(value)
                                .append("'"));

        return ddl.append("\n)").toString();
    }
}
