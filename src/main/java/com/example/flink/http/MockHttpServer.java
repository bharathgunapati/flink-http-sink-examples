package com.example.flink.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

final class MockHttpServer implements AutoCloseable {

    static final int PORT = 18080;
    static final String BASE_URL = "http://localhost:" + PORT;

    private final HttpServer server;
    private final AtomicInteger retryThenOkCalls = new AtomicInteger();

    private MockHttpServer(HttpServer server) {
        this.server = server;
    }

    static MockHttpServer start() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
            MockHttpServer mockServer = new MockHttpServer(server);
            server.createContext("/ok", exchange -> mockServer.respond(exchange, 200));
            server.createContext("/retry-then-ok", mockServer::retryThenOk);
            server.createContext("/always-fail", exchange -> mockServer.respond(exchange, 500));
            server.createContext("/ignored", exchange -> mockServer.respond(exchange, 404));
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            System.out.println("Mock HTTP server started at " + BASE_URL);
            return mockServer;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to start mock HTTP server", e);
        }
    }

    private void retryThenOk(HttpExchange exchange) throws IOException {
        int call = retryThenOkCalls.incrementAndGet();
        respond(exchange, call == 1 ? 500 : 200);
    }

    private void respond(HttpExchange exchange, int statusCode) throws IOException {
        byte[] requestBody = exchange.getRequestBody().readAllBytes();
        System.out.printf(
                "%s %s -> %d body=%s%n",
                exchange.getRequestMethod(),
                exchange.getRequestURI(),
                statusCode,
                new String(requestBody, StandardCharsets.UTF_8));

        byte[] responseBody = ("{\"status\":" + statusCode + "}").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, responseBody.length);
        exchange.getResponseBody().write(responseBody);
        exchange.close();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
