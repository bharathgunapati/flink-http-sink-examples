package com.example.flink.http;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public final class TableApiHttpSinkExample {

    private TableApiHttpSinkExample() {}

    public static void main(String[] args) throws Exception {
        try (MockHttpServer ignored = MockHttpServer.start()) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            tableEnv.executeSql(
                    "CREATE TABLE http_events (\n"
                            + "  id BIGINT,\n"
                            + "  event_type STRING\n"
                            + ") WITH (\n"
                            + "  'connector' = 'http',\n"
                            + "  'url' = '"
                            + MockHttpServer.BASE_URL
                            + "/retry-then-ok',\n"
                            + "  'format' = 'json',\n"
                            + "  'http.sink.writer.request.mode' = 'batch',\n"
                            + "  'http.sink.request.batch.size' = '10',\n"
                            + "  'http.sink.header.Content-Type' = 'application/json',\n"
                            + "  'http.sink.max-retries' = '3',\n"
                            + "  'http.sink.retry-codes' = '500,503,504',\n"
                            + "  'http.sink.retry-strategy.type' = 'fixed-delay',\n"
                            + "  'http.sink.retry-strategy.fixed-delay.delay' = '250ms'\n"
                            + ")");

            tableEnv.executeSql(
                            "INSERT INTO http_events VALUES "
                                    + "(1, 'table-api'), "
                                    + "(2, 'batch-retry')")
                    .await();
        }
    }
}
