/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dbeaver.jdbc.driver.libsql;

import com.dbeaver.jdbc.driver.libsql.client.LibSqlClient;
import com.sun.net.httpserver.HttpServer;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class LibSqlNamespaceTest {
    private final BlockingQueue<Request> requests = new LinkedBlockingQueue<>();
    private HttpServer server;

    @BeforeEach
    public void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(new Request(
                exchange.getRequestHeaders().getFirst("x-namespace-bin"),
                exchange.getRequestHeaders().getFirst("Authorization"),
                exchange.getRequestURI().toString(),
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            byte[] response = (exchange.getRequestMethod().equals("POST")
                ? "[{\"results\":{\"columns\":[\"value\"],\"rows\":[[1]]}}]"
                : "libsql 0.24.0 test").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (var output = exchange.getResponseBody()) {
                output.write(response);
            } finally {
                exchange.close();
            }
        });
        server.start();
    }

    @AfterEach
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void namespaceAndTokenReachValidationStatementsBatchesAndMetadata() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("db_name", "tenant-one");
        properties.setProperty("password", "test-token");
        try (Connection connection = new LibSqlDriver().connect(jdbcUrl(), properties)) {
            assertRequest("tenant-one", "Bearer test-token", "/");
            try (Statement statement = connection.createStatement()) {
                statement.executeQuery("SELECT 1");
                assertRequest("tenant-one", "Bearer test-token", "/");
            }
            try (PreparedStatement statement = connection.prepareStatement("SELECT ?")) {
                statement.setInt(1, 42);
                statement.executeQuery();
                Request request = assertRequest("tenant-one", "Bearer test-token", "/");
                Assertions.assertTrue(request.body().contains("\"params\":[42]"));
            }
            ((LibSqlConnection) connection).getClient().executeBatch(new String[] {"SELECT 1", "SELECT 2"}, null);
            Request batch = assertRequest("tenant-one", "Bearer test-token", "/");
            Assertions.assertTrue(batch.body().contains("\"statements\":[\"SELECT 1\",\"SELECT 2\"]"));
            Assertions.assertEquals("0.24.0", connection.getMetaData().getDatabaseProductVersion());
            assertRequest("tenant-one", "Bearer test-token", "/version");
        }
    }

    @Test
    public void urlNamespaceIsDecodedAndNotForwardedInQuery() throws Exception {
        try (Connection connection = new LibSqlDriver().connect(jdbcUrl() + "?db_name=tenant%2Bone", new Properties())) {
            Assertions.assertFalse(connection.isClosed());
            assertRequest("tenant+one", null, "/");
        }
    }

    @Test
    public void inheritedConnectionPropertyOverridesUrlWithoutMutatingCaller() throws Exception {
        Properties defaults = new Properties();
        defaults.setProperty("db_name", "tenant-two");
        Properties properties = new Properties(defaults);
        try (Connection connection = new LibSqlDriver().connect(jdbcUrl() + "?db_name=tenant-one", properties)) {
            Assertions.assertFalse(connection.isClosed());
            assertRequest("tenant-two", null, "/");
            Assertions.assertTrue(properties.isEmpty());
        }
    }

    @Test
    public void namespaceIsIsolatedPerConnection() throws Exception {
        LibSqlDriver driver = new LibSqlDriver();
        try (Connection first = driver.connect(jdbcUrl() + "?db_name=tenant-one", new Properties());
             Connection second = driver.connect(jdbcUrl() + "?db_name=tenant-two", new Properties())) {
            assertRequest("tenant-one", null, "/");
            assertRequest("tenant-two", null, "/");
            try (Statement statement = first.createStatement()) {
                statement.executeQuery("SELECT 1");
                assertRequest("tenant-one", null, "/");
            }
            try (Statement statement = second.createStatement()) {
                statement.executeQuery("SELECT 1");
                assertRequest("tenant-two", null, "/");
            }
        }
    }

    @Test
    public void unspecifiedAndEmptyNamespacesPreserveDefaultRouting() throws Exception {
        for (String suffix : new String[] {"", "?db_name="}) {
            try (Connection connection = new LibSqlDriver().connect(jdbcUrl() + suffix, new Properties())) {
                Assertions.assertFalse(connection.isClosed());
                assertRequest(null, null, "/");
            }
        }
    }

    @Test
    public void legacyClientConstructorPreservesDefaultRouting() throws Exception {
        LibSqlClient client = new LibSqlClient(URI.create(jdbcUrl().substring("jdbc:dbeaver:libsql:".length())).toURL(), null);
        try {
            client.execute("SELECT 1", null);
            assertRequest(null, null, "/");
        } finally {
            client.close();
        }
    }

    @NotNull
    private String jdbcUrl() {
        return "jdbc:dbeaver:libsql:http://127.0.0.1:" + server.getAddress().getPort();
    }

    @NotNull
    private Request assertRequest(@Nullable String namespace, @Nullable String authorization, @NotNull String path)
        throws InterruptedException {
        Request request = requests.poll(5, TimeUnit.SECONDS);
        Assertions.assertNotNull(request);
        Assertions.assertEquals(namespace, request.namespace() == null ? null
            : new String(Base64.getDecoder().decode(request.namespace()), StandardCharsets.UTF_8));
        if (request.namespace() != null) {
            Assertions.assertFalse(request.namespace().contains("="));
        }
        Assertions.assertEquals(authorization, request.authorization());
        Assertions.assertEquals(path, request.path());
        return request;
    }

    private record Request(@Nullable String namespace, @Nullable String authorization, @NotNull String path, @NotNull String body) {
    }
}
