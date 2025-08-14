/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2025 DBeaver Corp
 *
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of DBeaver Corp and its suppliers, if any.
 * The intellectual and technical concepts contained
 * herein are proprietary to DBeaver Corp and its suppliers
 * and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from DBeaver Corp.
 */
package com.dbeaver.jdbc.driver.libsql.client;

import com.dbeaver.jdbc.driver.libsql.LibSqlConstants;
import com.google.gson.*;
import com.google.gson.stream.JsonWriter;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.utils.CommonUtils;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.CookieManager;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The entry point to LibSQL client API.
 */
public class LibSqlClient {

    private static final Gson gson = new GsonBuilder()
        .setStrictness(Strictness.LENIENT)
        .setDateFormat(LibSqlConstants.DEFAULT_ISO_TIMESTAMP_FORMAT)
        .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
        .create();

    private final URL url;
    private final String authToken;
    private final ExecutorService clientExecutor;
    private String userAgent = LibSqlConstants.DRIVER_INFO + " " +
        LibSqlConstants.DRIVER_VERSION_MAJOR +
        "." + LibSqlConstants.DRIVER_VERSION_MAJOR +
        "." + LibSqlConstants.DRIVER_VERSION_MICRO;
    private final HttpClient client;

    public LibSqlClient(URL url, String authToken) {
        this.url = url;
        this.authToken = authToken;

        this.clientExecutor = Executors.newSingleThreadExecutor();
        HttpClient.Builder builder = HttpClient.newBuilder()
            .executor(this.clientExecutor)
            .cookieHandler(new CookieManager());
        this.client = builder.build();
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    /**
     * Execute a single SQL statement.
     *
     * @return The result set.
     */
    public LibSqlExecutionResult execute(String stmt, Map<Object, Object> parameters) throws SQLException {
        return executeBatch(new String[]{stmt}, new Map[]{ parameters })[0];
    }

    /**
     * Execute a batch of SQL statements.
     */
    public LibSqlExecutionResult[] executeBatch(
        @NotNull String[] stmts,
        @Nullable Map<Object, Object>[] parameters) throws SQLException {
        try {
            StringWriter requestBuffer = new StringWriter();
            executeQuery(stmts, parameters, requestBuffer);

            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(url.toURI())
                .version(HttpClient.Version.HTTP_1_1)
                .header("Content-Type", "application/json")
                .header("User-Agent", userAgent)
                .POST(HttpRequest.BodyPublishers.ofString(requestBuffer.toString()));
            if (!CommonUtils.isEmpty(authToken)) {
                builder.header("Authorization", "Bearer " + authToken);
            }

            HttpResponse.BodyHandler<String> readerBodyHandler =
                info -> HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8);

            final HttpResponse<String> httpResponse = client.send(
                builder.build(),
                readerBodyHandler
            );
            try {
                String responseBody = httpResponse.body();
                try (StringReader isr = new StringReader(responseBody)) {
                    Response[] response;
                    if (responseBody.startsWith("[")) {
                        response = gson.fromJson(isr, Response[].class);
                    } else {
                        Response parsedResponse;
                        try {
                            parsedResponse = gson.fromJson(isr, Response.class);
                        } catch (JsonSyntaxException e) {
                            parsedResponse = new Response();
                            parsedResponse.error = responseBody;
                        }
                        response = new Response[] { parsedResponse };
                    }
                    LibSqlExecutionResult[] resultSets = new LibSqlExecutionResult[response.length];
                    for (int i = 0; i < response.length; i++) {
                        if (!CommonUtils.isEmpty(response[i].error)) {
                            throw new SQLException(response[i].error);
                        }
                        resultSets[i] = response[i].results;
                    }
                    return resultSets;
                }
            } catch (Exception e) {
                switch (httpResponse.statusCode()) {
                    case HttpURLConnection.HTTP_UNAUTHORIZED:
                        throw new SQLException("Authentication required", e);
                    case HttpURLConnection.HTTP_FORBIDDEN:
                        throw new SQLException("Access denied", e);
                }
                throw e;
            }
        } catch (Exception e) {
            if (e instanceof SQLException) {
                SQLException sqle = (SQLException) e;
                throw sqle;
            }
            throw new SQLException(e);
        }

    }

    public HttpURLConnection openSimpleConnection(String endpoint) throws IOException {
        String baseURL = url.toString();
        if (!baseURL.endsWith("/")) {
            baseURL += "/";
        }
        baseURL += endpoint;
        HttpURLConnection connection = (HttpURLConnection) new URL(baseURL).openConnection();
        connection.setRequestProperty("User-Agent", userAgent);
        return connection;
    }

    private void executeQuery(
        @NotNull String[] queries,
        @Nullable Map<Object, Object>[] parameters,
        @NotNull Writer os
    ) throws IOException {
        JsonWriter jsonWriter = new JsonWriter(os);
        jsonWriter.beginObject();
        jsonWriter.name("statements");
        jsonWriter.beginArray();
        for (int i = 0; i < queries.length; i++) {
            String stmt = queries[i];
            if (parameters != null && i < parameters.length && !CommonUtils.isEmpty(parameters[i])) {
                // Query with parameters
                jsonWriter.beginObject();
                jsonWriter.name("q");
                jsonWriter.value(stmt);
                jsonWriter.name("params");
                if (isIndexedParams(parameters[i])) {
                    Map<Integer, Object> paramTree = new TreeMap<>();
                    for (Map.Entry<?,?> entry : parameters[i].entrySet()) {
                        paramTree.put((Integer) entry.getKey(), entry.getValue());
                    }
                    jsonWriter.beginArray();
                    for (Object value : paramTree.values()) {
                        serializeParameterValue(value, jsonWriter);
                    }
                    jsonWriter.endArray();
                } else {
                    jsonWriter.beginObject();
                    for (Map.Entry<?, ?> param : parameters[i].entrySet()) {
                        jsonWriter.name(String.valueOf(param.getKey()));
                        serializeParameterValue(param.getValue(), jsonWriter);
                    }
                    jsonWriter.endObject();
                }

                jsonWriter.endObject();
            } else {
                // Simple query
                jsonWriter.value(stmt);
            }
        }
        jsonWriter.endArray();
        jsonWriter.endObject();
        jsonWriter.flush();
    }

    private boolean isIndexedParams(Map<Object, Object> parameter) {
        if (!parameter.isEmpty()) {
            return parameter.keySet().iterator().next() instanceof Integer;
        }
        return false;
    }

    private static void serializeParameterValue(Object value, JsonWriter jsonWriter) throws IOException {
        if (value == null) {
            jsonWriter.nullValue();
        } else if (value instanceof Number) {
            Number nValue = (Number) value;
            jsonWriter.value(nValue);
        } else if (value instanceof Boolean) {
            Boolean bValue = (Boolean) value;
            jsonWriter.value(bValue);
        } else if (value instanceof String) {
            String strValue = (String) value;
            jsonWriter.value(strValue);
        } else {
            jsonWriter.value(value.toString());
        }
    }

    /**
     * Closes client. Terminates client executor.
     */
    public void close() {
        clientExecutor.shutdown();
    }

    private static class Response {
        public String error;
        public LibSqlExecutionResult results;
    }

}