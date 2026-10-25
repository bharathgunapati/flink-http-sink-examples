package com.example.flink.http;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Embedded HTTP server used by the examples and tests. Besides acting as the sink target it can
 * play an OIDC token endpoint, a bearer-protected endpoint and an authenticating HTTP proxy, so the
 * sink's authentication, proxy and HTTP version options can be observed end to end.
 */
final class MockHttpServer implements AutoCloseable {

    static final int PORT = 18080;
    static final String BASE_URL = "http://localhost:" + PORT;

    private final HttpServer server;
    private final ExecutorService executorService;
    private final Map<String, Responder> responders = new ConcurrentHashMap<>();
    private final Map<String, List<RecordedRequest>> recordedRequests = new ConcurrentHashMap<>();

    private MockHttpServer(HttpServer server, ExecutorService executorService) {
        this.server = server;
        this.executorService = executorService;
    }

    static MockHttpServer start() {
        MockHttpServer server = start(PORT);
        server.registerExampleEndpoints();
        return server;
    }

    static MockHttpServer startOnRandomPort() {
        return start(0);
    }

    private static MockHttpServer start(int port) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            ExecutorService executorService = Executors.newCachedThreadPool();
            MockHttpServer mockServer = new MockHttpServer(server, executorService);
            server.createContext("/", mockServer::route);
            server.setExecutor(executorService);
            server.start();
            System.out.println("Mock HTTP server started at " + mockServer.baseUrl());
            return mockServer;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to start mock HTTP server", e);
        }
    }

    String baseUrl() {
        return "http://localhost:" + port();
    }

    int port() {
        return server.getAddress().getPort();
    }

    /** Answers the n-th request with the n-th status code; the last code repeats forever. */
    void registerSequence(String path, int... statusCodes) {
        if (statusCodes.length == 0) {
            throw new IllegalArgumentException("At least one status code is required.");
        }
        List<Integer> codes = Arrays.stream(statusCodes).boxed().collect(Collectors.toList());
        AtomicInteger calls = new AtomicInteger();
        responders.put(
                path,
                request ->
                        Response.status(
                                codes.get(Math.min(calls.getAndIncrement(), codes.size() - 1))));
    }

    /**
     * Answers with {@code failureStatus} the first time a request body contains each marker, and
     * with {@code 200} otherwise. This makes a failure follow a specific record instead of a
     * request position, which is what the split-batch scenarios need: concurrent HTTP requests of
     * one flush may reach the server in any order.
     */
    void registerFailOnceForBodiesContaining(String path, int failureStatus, String... markers) {
        if (markers.length == 0) {
            throw new IllegalArgumentException("At least one body marker is required.");
        }
        Set<String> pending = ConcurrentHashMap.newKeySet();
        pending.addAll(Arrays.asList(markers));
        responders.put(
                path,
                request -> {
                    for (String marker : pending) {
                        // remove() is atomic, so a marker fails exactly one request even when
                        // several requests carrying it arrive at the same time.
                        if (request.body().contains(marker) && pending.remove(marker)) {
                            return Response.status(failureStatus);
                        }
                    }
                    return Response.status(200);
                });
    }

    /**
     * Plays an OIDC token endpoint. Every call returns a new token {@code token-1}, {@code
     * token-2}, ... with the given lifetime, after an optional delay. The number of token requests
     * is {@link #requestCount(String)} for the path.
     */
    void registerTokenEndpoint(String path, int expiresInSeconds, long delayMillis) {
        AtomicInteger issued = new AtomicInteger();
        responders.put(
                path,
                request -> {
                    sleep(delayMillis);
                    String body =
                            "{\"access_token\":\"token-"
                                    + issued.incrementAndGet()
                                    + "\",\"expires_in\":"
                                    + expiresInSeconds
                                    + "}";
                    return Response.status(200).withBody(body);
                });
    }

    /** Answers {@code 200} only when the request carries a {@code Bearer token-...} header. */
    void registerBearerProtected(String path) {
        responders.put(
                path,
                request ->
                        request.header("Authorization")
                                        .filter(value -> value.startsWith("Bearer token-"))
                                        .isPresent()
                                ? Response.status(200)
                                : Response.status(401));
    }

    /**
     * Plays an HTTP proxy that requires basic authentication: a request without {@code
     * Proxy-Authorization} gets {@code 407} with a challenge, a request with it gets {@code 200}.
     * The sink's proxy options point at this server and the sink URL at a host that does not
     * exist, so a {@code 200} proves the request went through the proxy.
     */
    void registerProxyRequiringBasicAuth(String path) {
        responders.put(
                path,
                request ->
                        request.header("Proxy-Authorization").isPresent()
                                ? Response.status(200)
                                : Response.status(407)
                                        .withHeader("Proxy-Authenticate", "Basic realm=\"mock\""));
    }

    int requestCount(String path) {
        return requests(path).size();
    }

    List<RecordedRequest> requests(String path) {
        return List.copyOf(recordedRequests.getOrDefault(path, Collections.emptyList()));
    }

    private void registerExampleEndpoints() {
        registerSequence("/ok", 200);
        registerSequence("/retry-then-ok", 500, 200);
        registerSequence("/always-fail", 500);
        registerSequence("/ignored", 404);
        registerFailOnceForBodiesContaining("/split-batch-retry", 500, "\"id\":1");
        registerTokenEndpoint("/oauth/token", 3600, 0);
        registerBearerProtected("/oidc-protected");
    }

    private void route(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        RecordedRequest request =
                new RecordedRequest(
                        exchange.getRequestMethod(),
                        path,
                        exchange.getRequestURI().toString(),
                        exchange.getProtocol(),
                        copyHeaders(exchange.getRequestHeaders()),
                        body);
        Responder responder = responders.get(path);
        Response response = responder == null ? Response.status(404) : responder.respond(request);
        record(request.withResponseStatus(response.status));
        sendResponse(exchange, response);
    }

    private void record(RecordedRequest request) {
        recordedRequests
                .computeIfAbsent(
                        request.path(), ignored -> Collections.synchronizedList(new ArrayList<>()))
                .add(request);
        System.out.printf(
                "%s %s -> %d%s body=%s%n",
                request.method(),
                request.rawUri(),
                request.responseStatus(),
                request.header("Authorization")
                        .map(value -> " authorization=" + value)
                        .orElse(""),
                request.body());
    }

    private static void sendResponse(HttpExchange exchange, Response response)
            throws IOException {
        byte[] responseBody = response.body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        response.headers.forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
        exchange.sendResponseHeaders(response.status, responseBody.length);
        exchange.getResponseBody().write(responseBody);
        exchange.close();
    }

    private static Map<String, List<String>> copyHeaders(Headers headers) {
        Map<String, List<String>> copy = new LinkedHashMap<>();
        headers.forEach(
                (name, values) -> copy.put(name.toLowerCase(Locale.ROOT), List.copyOf(values)));
        return Collections.unmodifiableMap(copy);
    }

    private static void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executorService.shutdownNow();
    }

    @FunctionalInterface
    private interface Responder {
        Response respond(RecordedRequest request);
    }

    private static final class Response {
        private final int status;
        private final Map<String, String> headers = new LinkedHashMap<>();
        private String body;

        private Response(int status) {
            this.status = status;
            this.body = "{\"status\":" + status + "}";
        }

        private static Response status(int status) {
            return new Response(status);
        }

        private Response withBody(String body) {
            this.body = body;
            return this;
        }

        private Response withHeader(String name, String value) {
            headers.put(name, value);
            return this;
        }
    }

    static final class RecordedRequest {
        private final String method;
        private final String path;
        private final String rawUri;
        private final String protocol;
        private final Map<String, List<String>> headers;
        private final String body;
        private final int responseStatus;

        private RecordedRequest(
                String method,
                String path,
                String rawUri,
                String protocol,
                Map<String, List<String>> headers,
                String body) {
            this(method, path, rawUri, protocol, headers, body, -1);
        }

        private RecordedRequest(
                String method,
                String path,
                String rawUri,
                String protocol,
                Map<String, List<String>> headers,
                String body,
                int responseStatus) {
            this.method = method;
            this.path = path;
            this.rawUri = rawUri;
            this.protocol = protocol;
            this.headers = headers;
            this.body = body;
            this.responseStatus = responseStatus;
        }

        private RecordedRequest withResponseStatus(int status) {
            return new RecordedRequest(method, path, rawUri, protocol, headers, body, status);
        }

        String method() {
            return method;
        }

        String path() {
            return path;
        }

        /** The request-target as sent: a path normally, an absolute URI when sent to a proxy. */
        String rawUri() {
            return rawUri;
        }

        String protocol() {
            return protocol;
        }

        /** First value of a request header, matched case-insensitively. */
        java.util.Optional<String> header(String name) {
            List<String> values = headers.get(name.toLowerCase(Locale.ROOT));
            return values == null || values.isEmpty()
                    ? java.util.Optional.empty()
                    : java.util.Optional.of(values.get(0));
        }

        /** All values of a request header, matched case-insensitively. */
        List<String> headerValues(String name) {
            return headers.getOrDefault(name.toLowerCase(Locale.ROOT), Collections.emptyList());
        }

        int responseStatus() {
            return responseStatus;
        }

        String body() {
            return body;
        }
    }
}
