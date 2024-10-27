package com.dbeaver.jdbc.driver.libsql.client;

import com.dbeaver.jdbc.driver.libsql.LSqlConstants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.Strictness;
import com.google.gson.ToNumberPolicy;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

/**
 * The entry point to LibSQL client API.
 */
public class LSqlClient {

    private static final Gson gson = new GsonBuilder()
        .setStrictness(Strictness.LENIENT)
        .setDateFormat(LSqlConstants.DEFAULT_ISO_TIMESTAMP_FORMAT)
        .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
        .create();

    private final URL url;
    private final String authToken;

    public LSqlClient(URL url, String authToken) {
        this.url = url;
        this.authToken = authToken;
    }

    /**
     * Execute a single SQL statement.
     *
     * @return The result set.
     */
    public LSqlExecutionResult execute(String stmt) throws SQLException {
        return batch(new String[]{stmt})[0];
    }

    /**
     * Execute a batch of SQL statements.
     *
     * @param stmts The SQL statements.
     * @return The result sets.
     */
    public LSqlExecutionResult[] batch(String[] stmts) throws SQLException {
        try {
            HttpURLConnection conn = openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                query(stmts, os);
            }
            try (InputStreamReader in = new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8)) {
                Response[] response = gson.fromJson(in, Response[].class);
                LSqlExecutionResult[] resultSets = new LSqlExecutionResult[response.length];
                for (int i = 0; i < response.length; i++) {
                    resultSets[i] = response[i].results;
                }
                return resultSets;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }

    }

    private HttpURLConnection openConnection() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) this.url.openConnection();
        if (authToken != null) {
            conn.setRequestProperty("Authorization", "Bearer " + authToken);
        }
        return conn;
    }

    private void query(String[] stmts, OutputStream os) throws IOException {
        JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
        jsonWriter.beginObject();
        jsonWriter.name("statements");
        jsonWriter.beginArray();
        for (String stmt : stmts) {
            jsonWriter.value(stmt);
        }
        jsonWriter.endArray();
        jsonWriter.endObject();
        jsonWriter.flush();
    }

    private static class Response {
        public LSqlExecutionResult results;
    }

}