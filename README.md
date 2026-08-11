# Flink HTTP Sink Examples

Small runnable examples for the Apache Flink HTTP sink connector.

The project demonstrates:

- DataStream API usage with `HttpSink.builder()`
- Table API / SQL DDL usage with `connector = 'http-async-sink'`
- fixed-delay retries
- retryable, ignored, and fatal response status codes
- single-request and batch-request sink modes

Each example starts an embedded HTTP server on `localhost:18080`.

## Prerequisites

- Java 11 or newer
- Maven
- A locally installed `org.apache.flink:flink-connector-http:1.1-SNAPSHOT`

From a checkout of `apache/flink-connector-http`, install the connector first:

```bash
mvn -pl flink-connector-http -DskipTests install
```

## Run DataStream Example

```bash
mvn package
java -cp target/flink-http-sink-examples-1.0-SNAPSHOT.jar \
  com.example.flink.http.DataStreamHttpSinkExample
```

The DataStream example writes JSON records to:

- `/retry-then-ok`: returns `500` once, then `200`
- `/ignored`: returns `404`, configured as ignored/success-like

## Run Table API Example

```bash
mvn package
java -cp target/flink-http-sink-examples-1.0-SNAPSHOT.jar \
  com.example.flink.http.TableApiHttpSinkExample
```

The Table API example creates an HTTP sink table with SQL DDL using
`connector = 'http-async-sink'` and inserts sample rows. It uses batch request mode and retries a
transient `500` response.

## Mock Endpoints

The embedded mock server exposes:

- `POST /ok`: always `200`
- `POST /retry-then-ok`: first request `500`, subsequent requests `200`
- `POST /always-fail`: always `500`
- `POST /ignored`: always `404`

The server logs every request body so retry behavior is visible from the console.
