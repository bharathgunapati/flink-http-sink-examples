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
    private final AtomicInteger retryThenOkCalls = new AtomicInteger();

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

    void registerSequence(String path, int... statusCodes) {
        endpointBehaviors.put(path, new EndpointBehavior(statusCodes));
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
    }

    private void route(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        EndpointBehavior behavior = endpointBehaviors.get(path);
        if (behavior == null) {
            record(exchange, 404);
            sendResponse(exchange, 404);
            return;
        }
        int statusCode = behavior.nextStatus();
        record(exchange, statusCode);
        sendResponse(exchange, statusCode);
    }

    private void retryThenOk(HttpExchange exchange) throws IOException {
        int call = retryThenOkCalls.incrementAndGet();
        respond(exchange, call == 1 ? 500 : 200);
    }

    private void respond(HttpExchange exchange, int statusCode) throws IOException {
        record(exchange, statusCode);
        sendResponse(exchange, statusCode);
    }

    private void record(HttpExchange exchange, int statusCode) throws IOException {
        byte[] requestBody = exchange.getRequestBody().readAllBytes();
        String body = new String(requestBody, StandardCharsets.UTF_8);
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

        private EndpointBehavior(int... statusCodes) {
            if (statusCodes.length == 0) {
                throw new IllegalArgumentException("At least one status code is required.");
            }
            this.statusCodes = Arrays.stream(statusCodes).boxed().collect(Collectors.toList());
        }

        private int nextStatus() {
            int index = calls.getAndIncrement();
            return statusCodes.get(Math.min(index, statusCodes.size() - 1));
        }
    }
}
