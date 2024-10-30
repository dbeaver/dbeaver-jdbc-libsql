package com.dbeaver.jdbc.driver.libsql.client;

import com.dbeaver.jdbc.driver.libsql.LibSqlConstants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.Strictness;
import com.google.gson.ToNumberPolicy;
import com.google.gson.stream.JsonWriter;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.utils.CommonUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

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

    public LibSqlClient(URL url, String authToken) {
        this.url = url;
        this.authToken = authToken;
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
            HttpURLConnection conn = openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                executeQuery(stmts, parameters, os);
            }
            try (InputStreamReader in = new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8)) {
                //String responseStr = IOUtils.readToString(in);
                Response[] response = gson.fromJson(in, Response[].class);
                LibSqlExecutionResult[] resultSets = new LibSqlExecutionResult[response.length];
                for (int i = 0; i < response.length; i++) {
                    if (!CommonUtils.isEmpty(response[i].error)) {
                        throw new SQLException(response[i].error);
                    }
                    resultSets[i] = response[i].results;
                }
                return resultSets;
            }
        } catch (IOException e) {
            throw new SQLException(e);
        }

    }

    public HttpURLConnection openConnection() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) this.url.openConnection();
        setAuthParameters(conn);
        return conn;
    }

    public HttpURLConnection openConnection(String endpoint) throws IOException {
        String baseURL = url.toString();
        if (!baseURL.endsWith("/")) {
            baseURL += "/";
        }
        baseURL += endpoint;
        HttpURLConnection conn = (HttpURLConnection) new URL(baseURL).openConnection();
        setAuthParameters(conn);
        return conn;
    }

    private void setAuthParameters(HttpURLConnection conn) {
        if (authToken != null) {
            conn.setRequestProperty("Authorization", "Bearer " + authToken);
        }
    }

    private void executeQuery(
        @NotNull String[] queries,
        @Nullable Map<Object, Object>[] parameters,
        @NotNull OutputStream os
    ) throws IOException {
        JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
        jsonWriter.beginObject();
        jsonWriter.name("statements");
        jsonWriter.beginArray();
        for (int i = 0; i < queries.length; i++) {
            String stmt = queries[i];
            if (i < parameters.length && !CommonUtils.isEmpty(parameters[i])) {
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
            Object key1 = parameter.keySet().iterator().next();
            if (key1 instanceof Integer) {
                return true;
            }
        }
        return false;
    }

    private static void serializeParameterValue(Object value, JsonWriter jsonWriter) throws IOException {
        if (value == null) {
            jsonWriter.nullValue();
        } else if (value instanceof Number nValue) {
            jsonWriter.value(nValue);
        } else if (value instanceof Boolean bValue) {
            jsonWriter.value(bValue);
        } else if (value instanceof String strValue) {
            jsonWriter.value(strValue);
        } else {
            jsonWriter.value(value.toString());
        }
    }

    private static class Response {
        public String error;
        public LibSqlExecutionResult results;
    }

}