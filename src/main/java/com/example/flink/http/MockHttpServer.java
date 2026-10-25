package com.example.flink.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

final class MockHttpServer implements AutoCloseable {

    static final int PORT = 18080;
    static final String BASE_URL = "http://localhost:" + PORT;

    private final HttpServer server;
    private final ExecutorService executorService;
    private final Map<String, EndpointBehavior> endpointBehaviors = new ConcurrentHashMap<>();
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
        return "http://localhost:" + server.getAddress().getPort();
    }

    /** Answers the n-th request with the n-th status code; the last code repeats forever. */
    void registerSequence(String path, int... statusCodes) {
        endpointBehaviors.put(path, EndpointBehavior.sequence(statusCodes));
    }

    /**
     * Answers with {@code failureStatus} the first time a request body contains each marker, and
     * with {@code 200} otherwise. This makes a failure follow a specific record instead of a
     * request position, which is what the split-batch scenarios need: concurrent HTTP requests of
     * one flush may reach the server in any order.
     */
    void registerFailOnceForBodiesContaining(String path, int failureStatus, String... markers) {
        endpointBehaviors.put(path, EndpointBehavior.failOnceForBodiesContaining(failureStatus, markers));
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
    }

    private void route(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        EndpointBehavior behavior = endpointBehaviors.get(path);
        int statusCode = behavior == null ? 404 : behavior.nextStatus(body);
        record(exchange, body, statusCode);
        sendResponse(exchange, statusCode);
    }

    private void record(HttpExchange exchange, String body, int statusCode) {
        String path = exchange.getRequestURI().getPath();
        recordedRequests
                .computeIfAbsent(path, ignored -> Collections.synchronizedList(new ArrayList<>()))
                .add(new RecordedRequest(exchange.getRequestMethod(), path, statusCode, body));
        System.out.printf(
                "%s %s -> %d body=%s%n",
                exchange.getRequestMethod(),
                exchange.getRequestURI(),
                statusCode,
                body);
    }

    private void sendResponse(HttpExchange exchange, int statusCode) throws IOException {
        byte[] responseBody = ("{\"status\":" + statusCode + "}").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, responseBody.length);
        exchange.getResponseBody().write(responseBody);
        exchange.close();
    }

    @Override
    public void close() {
        server.stop(0);
        executorService.shutdownNow();
    }

    static final class RecordedRequest {
        private final String method;
        private final String path;
        private final int responseStatus;
        private final String body;

        private RecordedRequest(String method, String path, int responseStatus, String body) {
            this.method = method;
            this.path = path;
            this.responseStatus = responseStatus;
            this.body = body;
        }

        String method() {
            return method;
        }

        String path() {
            return path;
        }

        int responseStatus() {
            return responseStatus;
        }

        String body() {
            return body;
        }
    }

    private static final class EndpointBehavior {
        private final List<Integer> statusCodes;
        private final AtomicInteger calls = new AtomicInteger();
        private final int failureStatus;
        private final Set<String> pendingFailureMarkers;

        private EndpointBehavior(
                List<Integer> statusCodes, int failureStatus, Set<String> pendingFailureMarkers) {
            this.statusCodes = statusCodes;
            this.failureStatus = failureStatus;
            this.pendingFailureMarkers = pendingFailureMarkers;
        }

        private static EndpointBehavior sequence(int... statusCodes) {
            if (statusCodes.length == 0) {
                throw new IllegalArgumentException("At least one status code is required.");
            }
            return new EndpointBehavior(
                    Arrays.stream(statusCodes).boxed().collect(Collectors.toList()), 0, null);
        }

        private static EndpointBehavior failOnceForBodiesContaining(
                int failureStatus, String... markers) {
            if (markers.length == 0) {
                throw new IllegalArgumentException("At least one body marker is required.");
            }
            Set<String> pending = ConcurrentHashMap.newKeySet();
            pending.addAll(Arrays.asList(markers));
            return new EndpointBehavior(List.of(200), failureStatus, pending);
        }

        private int nextStatus(String body) {
            if (pendingFailureMarkers != null) {
                for (String marker : pendingFailureMarkers) {
                    // remove() is atomic, so a marker fails exactly one request even when
                    // several requests carrying it arrive at the same time.
                    if (body.contains(marker) && pendingFailureMarkers.remove(marker)) {
                        return failureStatus;
                    }
                }
                return 200;
            }
            int index = calls.getAndIncrement();
            return statusCodes.get(Math.min(index, statusCodes.size() - 1));
        }
    }
}
