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

import org.jkiss.code.NotNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;

public class LibSqlUtils {

    public static String quote(String identifier) {
        return "'" + identifier + "'";
    }

    /**
     * Follow rules in <a href="https://www.sqlite.org/lang_keywords.html">SQLite Keywords</a>
     *
     * @param name Identifier name
     * @return Unquoted identifier
     */
    public static String unquote(String name) {
        if (name == null) return name;
        name = name.trim();
        if (name.length() > 2
            && ((name.startsWith("`") && name.endsWith("`"))
                || (name.startsWith("\"") && name.endsWith("\""))
                || (name.startsWith("[") && name.endsWith("]")))) {
            // unquote to be consistent with column names returned by getColumns()
            name = name.substring(1, name.length() - 1);
        }
        return name;
    }

    public static String escape(final String val) {
        if (val.indexOf('\'') == 1) {
            return val;
        }
        int len = val.length();
        StringBuilder buf = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            if (val.charAt(i) == '\'') {
                buf.append('\'');
            }
            buf.append(val.charAt(i));
        }
        return buf.toString();
    }

    public static ResultSet executeQuery(Connection connection, String query) throws SQLException {
        try (Statement stat = connection.createStatement()) {
            return stat.executeQuery(query);
        }
    }

    @NotNull
    public static String validateAndFormatUrl(@NotNull String url) throws LibSqlException {
        Matcher matcher = LibSqlConstants.CONNECTION_URL_PATTERN.matcher(url);
        if (!matcher.matches()) {
            throw new LibSqlException(
                "Invalid connection URL: " + url +
                    ".\nExpected URL formats: " + LibSqlConstants.CONNECTION_URL_EXAMPLES
            );
        }

        String formattedUrl = url;
        if (formattedUrl.startsWith("jdbc:dbeaver:libsql:")) {
            formattedUrl = formattedUrl.replaceFirst("jdbc:dbeaver:libsql:", "");
        }
        if (formattedUrl.startsWith("libsql://")) {
            formattedUrl = formattedUrl.replaceFirst("libsql://", "https://");
        }
        return formattedUrl;
    }

    /**
     * Extract JDBC-only URL properties without forwarding them to the HTTP server.
     * Explicit connection properties take precedence over URL values.
     */
    @NotNull
    static String parseUrlProperties(@NotNull String url, @NotNull Map<String, Object> properties) throws LibSqlException {
        String targetUrl = validateAndFormatUrl(url);
        try {
            URI uri = new URI(targetUrl);
            String query = uri.getRawQuery();
            if (query == null) {
                return targetUrl;
            }
            StringJoiner remaining = new StringJoiner("&");
            boolean found = false;
            for (String parameter : query.split("&", -1)) {
                String[] parts = parameter.split("=", 2);
                if (LibSqlConstants.PROP_DATABASE_NAME.equals(URLDecoder.decode(parts[0], StandardCharsets.UTF_8))) {
                    String value = parts.length == 2 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
                    properties.putIfAbsent(LibSqlConstants.PROP_DATABASE_NAME, value);
                    found = true;
                } else {
                    remaining.add(parameter);
                }
            }
            if (!found) {
                return targetUrl;
            }
            return targetUrl.substring(0, targetUrl.indexOf('?'))
                + (remaining.length() == 0 ? "" : "?" + remaining)
                + (uri.getRawFragment() == null ? "" : "#" + uri.getRawFragment());
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new LibSqlException("Invalid connection URL", e);
        }
    }
}
