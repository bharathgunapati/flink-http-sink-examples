# Flink HTTP Sink Examples

Small runnable examples for the Apache Flink HTTP sink connector.

The project demonstrates:

- DataStream API usage with `HttpSink.builder()`
- Table API / SQL DDL usage with `connector = 'http-async-sink'`
- fixed-delay retries
- exponential-delay retries
- retryable, ignored, and fatal response status codes
- single-request and batch-request sink modes

It also includes downstream integration tests for the HTTP sink retry/configuration behavior added
for PR 54.

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
- `/split-batch-retry`: three records with `http.sink.request.batch.size = 2`; the request carrying
  record 1 fails once, so the console shows only that request being resent and record 3 sent once
- `/oidc-protected`: the sink fetches a bearer token from `/oauth/token` once and sends it with
  every request; the endpoint answers `401` without it

## Run Table API Example

```bash
mvn package
java -cp target/flink-http-sink-examples-1.0-SNAPSHOT.jar \
  com.example.flink.http.TableApiHttpSinkExample
```

The Table API example creates an HTTP sink table with SQL DDL using
`connector = 'http-async-sink'` and inserts sample rows. It uses batch request mode and retries a
transient `500` response.

## Run Downstream Regression Tests

The tests install/use the locally published `flink-connector-http` artifact and run small Flink jobs
against an embedded HTTP server:

```bash
mvn verify
```

The integration suite covers:

- DataStream single-request retry: `500 -> 200`
- DataStream batch retry: `503 -> 200`, asserting the same JSON array payload is retried
- DataStream split flush: one flush split into several HTTP requests retries only the failed
  request's records, and retried records are regrouped into new requests (FLINK-40278)
- retry exhaustion and `http.sink.max-retries = 0`
- ignored response codes treated as success without retrying
- exponential-delay retry configuration
- Table API DDL coverage for batch retry, custom success codes, ignored codes, fatal statuses, and
  exponential-delay retry options
- Sink OIDC (FLINK-40279): the bearer token is fetched once and reused, refreshed when expired,
  replaces a configured `Authorization` header, and a token endpoint that does not answer within
  `http.sink.request.timeout` fails the job without sending any sink request
- Sink proxy with basic authentication: the mock plays a proxy that challenges with `407`, and the
  sink retries with `Proxy-Authorization` and the absolute target URI
- `http.sink.http-version = HTTP_2`: the client asks for an h2c upgrade over cleartext
- Table API DDL coverage for the OIDC options and for rejecting an unknown HTTP version

The GitHub Actions workflow checks out `bharathgunapati/flink-connector-http` branch
`FLINK-40279-sink-oidc-proxy-http-version`, installs the connector snapshot, then runs `mvn verify`.
That branch carries the retry behaviour from FLINK-40277 as well as the OIDC, proxy and HTTP
version options, so every scenario above runs against it.

## Mock Endpoints

The embedded mock server exposes:

- `POST /ok`: always `200`
- `POST /retry-then-ok`: first request `500`, subsequent requests `200`
- `POST /always-fail`: always `500`
- `POST /ignored`: always `404`
- `POST /split-batch-retry`: `500` for the first request whose body contains `"id":1`, `200` otherwise
- `POST /oauth/token`: OIDC token endpoint, returns `token-1`, `token-2`, ... with a one hour lifetime
- `POST /oidc-protected`: `200` with an `Authorization: Bearer token-...` header, `401` otherwise

The server logs every request body, and the `Authorization` header when present, so retry and
authentication behavior is visible from the console.
