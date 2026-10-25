package com.example.flink.http;

import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.HttpSinkBuilder;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.SinkRequestSubmitMode;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

class DataStreamHttpSinkIT {

    @Test
    void retriesTransientServerErrorInSingleRequestMode() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/single-retry", 500, 200);

            runDataStreamJob(
                    server.baseUrl() + "/single-retry",
                    List.of("{\"id\":1,\"api\":\"datastream\"}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE,
                            SinkRequestSubmitMode.SINGLE.getMode(),
                            HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                            "2",
                            HttpConnectorConfigConstants.SINK_RETRY_CODES,
                            "500",
                            HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY,
                            "10ms"));

            assertThat(server.requestCount("/single-retry")).isEqualTo(2);
            assertThat(server.requests("/single-retry"))
                    .extracting(MockHttpServer.RecordedRequest::body)
                    .containsExactly(
                            "{\"id\":1,\"api\":\"datastream\"}",
                            "{\"id\":1,\"api\":\"datastream\"}");
            assertThat(server.requests("/single-retry"))
                    .extracting(MockHttpServer.RecordedRequest::responseStatus)
                    .containsExactly(500, 200);
        }
    }

    @Test
    void retriesTransientServerErrorInBatchRequestMode() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/batch-retry", 503, 200);

            runDataStreamJob(
                    server.baseUrl() + "/batch-retry",
                    List.of(
                            "{\"id\":1,\"api\":\"datastream\"}",
                            "{\"id\":2,\"api\":\"datastream\"}"),
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

            assertThat(server.requestCount("/batch-retry")).isEqualTo(2);
            assertThat(server.requests("/batch-retry"))
                    .extracting(MockHttpServer.RecordedRequest::body)
                    .containsExactly(
                            "[{\"id\":1,\"api\":\"datastream\"},{\"id\":2,\"api\":\"datastream\"}]",
                            "[{\"id\":1,\"api\":\"datastream\"},{\"id\":2,\"api\":\"datastream\"}]");
        }
    }

    @Test
    void retriesOnlyTheFailedRequestWhenOneFlushIsSplitIntoSeveralHttpRequests()
            throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            // The first HTTP request that carries record 1 fails; every other request succeeds.
            server.registerFailOnceForBodiesContaining("/split-batch", 500, "\"id\":1");

            runDataStreamJob(
                    server.baseUrl() + "/split-batch",
                    List.of("{\"id\":1}", "{\"id\":2}", "{\"id\":3}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE,
                            SinkRequestSubmitMode.BATCH.getMode(),
                            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                            "2",
                            HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                            "2",
                            HttpConnectorConfigConstants.SINK_RETRY_CODES,
                            "500",
                            HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY,
                            "10ms"));

            List<MockHttpServer.RecordedRequest> requests = server.requests("/split-batch");
            // One flush of three records with batch size 2 becomes two HTTP requests. They are
            // sent concurrently, so the first two may arrive in either order.
            assertThat(requests).hasSize(3);
            assertThat(requests.subList(0, 2))
                    .extracting(
                            MockHttpServer.RecordedRequest::body,
                            MockHttpServer.RecordedRequest::responseStatus)
                    .containsExactlyInAnyOrder(
                            tuple("[{\"id\":1},{\"id\":2}]", 500), tuple("[{\"id\":3}]", 200));
            // Only the failed request is resent, with its original two records.
            assertThat(requests.get(2).body()).isEqualTo("[{\"id\":1},{\"id\":2}]");
            assertThat(requests.get(2).responseStatus()).isEqualTo(200);
            // Record 3 was accepted on the first attempt and is never sent again.
            assertThat(requests)
                    .extracting(MockHttpServer.RecordedRequest::body)
                    .filteredOn(body -> body.contains("\"id\":3"))
                    .hasSize(1);
        }
    }

    @Test
    void regroupsRetriedRecordsIntoNewHttpRequests() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            // Records 1 and 3 fail on their first request each; record 2 succeeds.
            server.registerFailOnceForBodiesContaining("/regroup", 500, "\"id\":1", "\"id\":3");

            runDataStreamJob(
                    server.baseUrl() + "/regroup",
                    List.of("{\"id\":1}", "{\"id\":2,\"method\":\"PUT\"}", "{\"id\":3}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE,
                            SinkRequestSubmitMode.BATCH.getMode(),
                            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                            "10",
                            HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                            "2",
                            HttpConnectorConfigConstants.SINK_RETRY_CODES,
                            "500",
                            HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY,
                            "10ms"));

            List<MockHttpServer.RecordedRequest> requests = server.requests("/regroup");
            // The PUT in the middle splits the first attempt into three HTTP requests.
            assertThat(requests).hasSize(4);
            assertThat(requests.subList(0, 3))
                    .extracting(
                            MockHttpServer.RecordedRequest::method,
                            MockHttpServer.RecordedRequest::body,
                            MockHttpServer.RecordedRequest::responseStatus)
                    .containsExactlyInAnyOrder(
                            tuple("POST", "[{\"id\":1}]", 500),
                            tuple("PUT", "[{\"id\":2,\"method\":\"PUT\"}]", 200),
                            tuple("POST", "[{\"id\":3}]", 500));
            // On retry the two failed POST records are adjacent and travel in one request.
            MockHttpServer.RecordedRequest retry = requests.get(3);
            assertThat(retry.method()).isEqualTo("POST");
            assertThat(retry.body()).isEqualTo("[{\"id\":1},{\"id\":3}]");
            assertThat(retry.responseStatus()).isEqualTo(200);
        }
    }

    @Test
    void disablesRetriesWhenMaxRetriesIsZero() {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/no-retry", 500);

            assertThatThrownBy(
                            () ->
                                    runDataStreamJob(
                                            server.baseUrl() + "/no-retry",
                                            List.of("{\"id\":1,\"api\":\"datastream\"}"),
                                            Map.of(
                                                    HttpConnectorConfigConstants
                                                            .SINK_HTTP_REQUEST_MODE,
                                                    SinkRequestSubmitMode.SINGLE.getMode(),
                                                    HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                                                    "0",
                                                    HttpConnectorConfigConstants.SINK_RETRY_CODES,
                                                    "500")))
                    .hasStackTraceContaining("HTTP sink exhausted retries");

            assertThat(server.requestCount("/no-retry")).isEqualTo(1);
        }
    }

    @Test
    void failsAfterRetryExhaustion() {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/always-fail", 504);

            assertThatThrownBy(
                            () ->
                                    runDataStreamJob(
                                            server.baseUrl() + "/always-fail",
                                            List.of("{\"id\":1,\"api\":\"datastream\"}"),
                                            Map.of(
                                                    HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                                                    "2",
                                                    HttpConnectorConfigConstants.SINK_RETRY_CODES,
                                                    "504",
                                                    HttpConnectorConfigConstants
                                                            .SINK_RETRY_FIXED_DELAY_DELAY,
                                                    "10ms")))
                    .hasStackTraceContaining("HTTP sink exhausted retries");

            assertThat(server.requestCount("/always-fail")).isEqualTo(3);
        }
    }

    @Test
    void treatsIgnoredStatusAsSuccessfulWithoutRetrying() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/ignored", 404);

            runDataStreamJob(
                    server.baseUrl() + "/ignored",
                    List.of("{\"id\":1,\"api\":\"datastream\"}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES,
                            "404",
                            HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                            "2"));

            assertThat(server.requestCount("/ignored")).isEqualTo(1);
        }
    }

    @Test
    void supportsExponentialDelayRetryConfiguration() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/exponential", 500, 500, 200);

            runDataStreamJob(
                    server.baseUrl() + "/exponential",
                    List.of("{\"id\":1,\"api\":\"datastream\"}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_MAX_RETRIES,
                            "3",
                            HttpConnectorConfigConstants.SINK_RETRY_CODES,
                            "500",
                            HttpConnectorConfigConstants.SINK_RETRY_STRATEGY_TYPE,
                            "exponential-delay",
                            HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_INITIAL_BACKOFF,
                            "10ms",
                            HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MAX_BACKOFF,
                            "100ms",
                            HttpConnectorConfigConstants.SINK_RETRY_EXP_DELAY_MULTIPLIER,
                            "2.0"));

            assertThat(server.requestCount("/exponential")).isEqualTo(3);
        }
    }

    @Test
    void supportsLegacyErrorCodeOptions() {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/legacy-fatal", 500);

            assertThatThrownBy(
                            () ->
                                    runDataStreamJob(
                                            server.baseUrl() + "/legacy-fatal",
                                            List.of("{\"id\":1,\"api\":\"datastream\"}"),
                                            Map.of(
                                                    HttpConnectorConfigConstants
                                                            .HTTP_ERROR_SINK_CODES_LIST,
                                                    "5XX")))
                    .hasStackTraceContaining("HTTP sink received fatal response status");

            assertThat(server.requestCount("/legacy-fatal")).isEqualTo(1);
        }
    }

    @Test
    void supportsLegacyExcludeAsIgnoredStatus() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/legacy-exclude", 404);

            runDataStreamJob(
                    server.baseUrl() + "/legacy-exclude",
                    List.of("{\"id\":1,\"api\":\"datastream\"}"),
                    Map.of(
                            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODES_LIST,
                            "4XX",
                            HttpConnectorConfigConstants.HTTP_ERROR_SINK_CODE_INCLUDE_LIST,
                            "404"));

            assertThat(server.requestCount("/legacy-exclude")).isEqualTo(1);
        }
    }

    @Test
    void rejectsMixedLegacyAndNewStatusCodeOptions() {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/mixed", 200);

            assertThatThrownBy(
                            () ->
                                    runDataStreamJob(
                                            server.baseUrl() + "/mixed",
                                            List.of("{\"id\":1,\"api\":\"datastream\"}"),
                                            Map.of(
                                                    HttpConnectorConfigConstants
                                                            .HTTP_ERROR_SINK_CODES_LIST,
                                                    "4XX",
                                                    HttpConnectorConfigConstants.SINK_SUCCESS_CODES,
                                                    "2XX")))
                    .hasStackTraceContaining(
                            "Cannot set legacy HTTP sink error-code properties");
        }
    }

    // ---- FLINK-40279: OIDC, proxy and HTTP version on the sink ----

    @Test
    void addsBearerTokenFromOidcAndReusesItWhileValid() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerTokenEndpoint("/oauth/token", 3600, 0);
            server.registerBearerProtected("/oidc");

            // batch size 1 turns three records into three HTTP requests through the batch submitter
            runDataStreamJob(
                    server.baseUrl() + "/oidc",
                    List.of("{\"id\":1}", "{\"id\":2}", "{\"id\":3}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE,
                            SinkRequestSubmitMode.BATCH.getMode(),
                            HttpConnectorConfigConstants.SINK_HTTP_BATCH_REQUEST_SIZE,
                            "1",
                            HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_ENDPOINT_URL,
                            server.baseUrl() + "/oauth/token",
                            HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_REQUEST,
                            "grant_type=client_credentials&client_id=sink"));

            // one token for the whole job, and the configured token request body was sent
            assertThat(server.requestCount("/oauth/token")).isEqualTo(1);
            assertThat(server.requests("/oauth/token").get(0).body())
                    .isEqualTo("grant_type=client_credentials&client_id=sink");
            assertThat(server.requests("/oidc"))
                    .hasSize(3)
                    .allSatisfy(
                            request -> {
                                assertThat(request.header("Authorization"))
                                        .contains("Bearer token-1");
                                assertThat(request.responseStatus()).isEqualTo(200);
                            });
        }
    }

    @Test
    void refreshesExpiredOidcTokenBeforeNextRequest() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            // expires_in 0: every token is expired as soon as it is issued
            server.registerTokenEndpoint("/oauth/token", 0, 0);
            server.registerBearerProtected("/oidc-refresh");

            runDataStreamJob(
                    server.baseUrl() + "/oidc-refresh",
                    List.of("{\"id\":1}", "{\"id\":2}", "{\"id\":3}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE,
                            SinkRequestSubmitMode.SINGLE.getMode(),
                            HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_ENDPOINT_URL,
                            server.baseUrl() + "/oauth/token",
                            HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_REQUEST,
                            "grant_type=client_credentials"));

            assertThat(server.requestCount("/oauth/token")).isEqualTo(3);
            assertThat(server.requests("/oidc-refresh"))
                    .extracting(request -> request.header("Authorization").orElse(null))
                    .containsExactlyInAnyOrder(
                            "Bearer token-1", "Bearer token-2", "Bearer token-3");
        }
    }

    @Test
    void replacesConfiguredAuthorizationHeaderWithOidcBearerToken() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerTokenEndpoint("/oauth/token", 3600, 0);
            server.registerBearerProtected("/oidc-replace");

            runDataStreamJob(
                    server.baseUrl() + "/oidc-replace",
                    List.of("{\"id\":1}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Authorization",
                            "Basic c3RhdGljOnNlY3JldA==",
                            HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_ENDPOINT_URL,
                            server.baseUrl() + "/oauth/token",
                            HttpConnectorConfigConstants.OIDC_AUTH_TOKEN_REQUEST,
                            "grant_type=client_credentials"));

            MockHttpServer.RecordedRequest request = server.requests("/oidc-replace").get(0);
            // the static header is gone, the bearer is the only Authorization value
            assertThat(request.headerValues("Authorization")).containsExactly("Bearer token-1");
            // other configured headers are untouched
            assertThat(request.header("Content-Type")).contains("application/json");
            assertThat(request.responseStatus()).isEqualTo(200);
        }
    }

    @Test
    void failsTheJobWhenTokenEndpointDoesNotAnswerWithinRequestTimeout() {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerTokenEndpoint("/oauth/token", 3600, 2_000);
            server.registerSequence("/oidc-timeout", 200);

            assertThatThrownBy(
                            () ->
                                    runDataStreamJob(
                                            server.baseUrl() + "/oidc-timeout",
                                            List.of("{\"id\":1}"),
                                            Map.of(
                                                    HttpConnectorConfigConstants
                                                            .SINK_HTTP_TIMEOUT_SECONDS,
                                                    "500ms",
                                                    HttpConnectorConfigConstants
                                                            .OIDC_AUTH_TOKEN_ENDPOINT_URL,
                                                    server.baseUrl() + "/oauth/token",
                                                    HttpConnectorConfigConstants
                                                            .OIDC_AUTH_TOKEN_REQUEST,
                                                    "grant_type=client_credentials")))
                    .hasStackTraceContaining("OIDC access token");

            // no sink request is sent without a token
            assertThat(server.requestCount("/oidc-timeout")).isZero();
        }
    }

    @Test
    void sendsRequestsThroughConfiguredProxyWithBasicAuthentication() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerProxyRequiringBasicAuth("/via-proxy");

            // the sink URL points at a host that does not exist; only a proxy can deliver it
            runDataStreamJob(
                    "http://sink.invalid:19999/via-proxy",
                    List.of("{\"id\":1}"),
                    Map.of(
                            HttpConnectorConfigConstants.SINK_PROXY_HOST,
                            "localhost",
                            HttpConnectorConfigConstants.SINK_PROXY_PORT,
                            String.valueOf(server.port()),
                            HttpConnectorConfigConstants.SINK_PROXY_USERNAME,
                            "proxy-user",
                            HttpConnectorConfigConstants.SINK_PROXY_PASSWORD,
                            "proxy-pass"));

            List<MockHttpServer.RecordedRequest> requests = server.requests("/via-proxy");
            // first attempt is challenged, second carries the proxy credentials
            assertThat(requests).hasSize(2);
            assertThat(requests.get(0).responseStatus()).isEqualTo(407);
            assertThat(requests.get(0).header("Proxy-Authorization")).isEmpty();
            assertThat(requests.get(1).responseStatus()).isEqualTo(200);
            assertThat(requests.get(1).header("Proxy-Authorization"))
                    .contains(
                            "Basic "
                                    + Base64.getEncoder()
                                            .encodeToString(
                                                    "proxy-user:proxy-pass"
                                                            .getBytes(StandardCharsets.UTF_8)));
            // a request sent to a proxy carries the absolute URI of the real target
            assertThat(requests.get(1).rawUri()).startsWith("http://sink.invalid:19999/via-proxy");
            assertThat(requests.get(1).body()).isEqualTo("[{\"id\":1}]");
        }
    }

    @Test
    void negotiatesHttp2WhenConfigured() throws Exception {
        try (MockHttpServer server = MockHttpServer.startOnRandomPort()) {
            server.registerSequence("/h2", 200);

            runDataStreamJob(
                    server.baseUrl() + "/h2",
                    List.of("{\"id\":1}"),
                    Map.of(HttpConnectorConfigConstants.SINK_HTTP_VERSION, "HTTP_2"));

            // over cleartext the JDK client asks for HTTP/2 with an h2c upgrade; the embedded
            // server only speaks HTTP/1.1, so it ignores the upgrade and answers normally
            MockHttpServer.RecordedRequest request = server.requests("/h2").get(0);
            assertThat(request.header("Upgrade")).contains("h2c");
            assertThat(request.responseStatus()).isEqualTo(200);
        }
    }

    private static void runDataStreamJob(
            String endpointUrl, List<String> payloads, Map<String, String> properties)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        HttpSinkBuilder<String> builder =
                HttpSink.<String>builder()
                        .setEndpointUrl(endpointUrl)
                        .setElementConverter(
                                (event, context) ->
                                        new HttpSinkRequestEntry(
                                                methodFor(event),
                                                event.getBytes(StandardCharsets.UTF_8)))
                        .setProperty(
                                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Content-Type",
                                "application/json");

        properties.forEach(builder::setProperty);

        env.fromCollection(payloads).sinkTo(builder.build());
        env.execute("DataStream HTTP sink scenario");
    }

    /** Records that carry {@code "method":"PUT"} are sent with PUT, everything else with POST. */
    private static String methodFor(String event) {
        return event.contains("\"method\":\"PUT\"") ? "PUT" : "POST";
    }
}
