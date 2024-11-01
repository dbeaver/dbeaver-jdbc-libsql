/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp and others
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
}
