package com.example.flink.http;

import org.apache.flink.connector.http.HttpSink;
import org.apache.flink.connector.http.config.HttpConnectorConfigConstants;
import org.apache.flink.connector.http.config.SinkRequestSubmitMode;
import org.apache.flink.connector.http.sink.HttpSinkRequestEntry;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.util.List;

public final class DataStreamHttpSinkExample {

    private DataStreamHttpSinkExample() {}

    public static void main(String[] args) throws Exception {
        try (MockHttpServer ignored = MockHttpServer.start()) {
            runRetryingSink();
            runIgnoredStatusSink();
        }
    }

    private static void runRetryingSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        HttpSink<String> sink =
                HttpSink.<String>builder()
                        .setEndpointUrl(MockHttpServer.BASE_URL + "/retry-then-ok")
                        .setElementConverter(
                                (event, context) ->
                                        new HttpSinkRequestEntry(
                                                "POST", event.getBytes(StandardCharsets.UTF_8)))
                        .setProperty(
                                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Content-Type",
                                "application/json")
                        .setProperty(
                                HttpConnectorConfigConstants.SINK_HTTP_REQUEST_MODE,
                                SinkRequestSubmitMode.SINGLE.getMode())
                        .setProperty(HttpConnectorConfigConstants.SINK_MAX_RETRIES, "3")
                        .setProperty(HttpConnectorConfigConstants.SINK_RETRY_CODES, "500,503,504")
                        .setProperty(
                                HttpConnectorConfigConstants.SINK_RETRY_FIXED_DELAY_DELAY, "250ms")
                        .build();

        env.fromCollection(List.of("{\"id\":1,\"kind\":\"retry-demo\"}")).sinkTo(sink);
        env.execute("DataStream HTTP sink retry example");
    }

    private static void runIgnoredStatusSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        HttpSink<String> sink =
                HttpSink.<String>builder()
                        .setEndpointUrl(MockHttpServer.BASE_URL + "/ignored")
                        .setElementConverter(
                                (event, context) ->
                                        new HttpSinkRequestEntry(
                                                "POST", event.getBytes(StandardCharsets.UTF_8)))
                        .setProperty(
                                HttpConnectorConfigConstants.SINK_HEADER_PREFIX + "Content-Type",
                                "application/json")
                        .setProperty(
                                HttpConnectorConfigConstants.SINK_IGNORE_RESPONSE_CODES, "404")
                        .build();

        env.fromCollection(List.of("{\"id\":2,\"kind\":\"ignored-status-demo\"}")).sinkTo(sink);
        env.execute("DataStream HTTP sink ignored status example");
    }
}
