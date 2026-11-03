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
import org.jkiss.code.Nullable;

import java.sql.*;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LibSqlDriver implements Driver {

    static {
        Logger logger = Logger.getLogger("com.dbeaver.jdbc.upd.driver.driver");
        parentLogger = logger;
        try {
            DriverManager.registerDriver(new LibSqlDriver());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Could not register driver", e);
        }
    }

    private static final java.util.logging.Logger parentLogger;

    @Override
    @NotNull
    public Connection connect(@NotNull String url, @NotNull Properties info) throws SQLException {
        Map<String, Object> props = new LinkedHashMap<>();
        for (Enumeration<?> pne = info.propertyNames(); pne.hasMoreElements(); ) {
            String propName = (String) pne.nextElement();
            props.put(propName, info.getOrDefault(propName, info.getProperty(propName)));
        }
        String targetUrl = LibSqlUtils.parseUrlProperties(url, props);
        return new LibSqlConnection(this, targetUrl, props);
    }

    @Override
    public boolean acceptsURL(String url) {
        return LibSqlConstants.CONNECTION_URL_PATTERN.matcher(url).matches();
    }

    @Override
    @NotNull
    public DriverPropertyInfo[] getPropertyInfo(@Nullable String url, @NotNull Properties info) throws SQLException {
        Map<String, Object> props = new LinkedHashMap<>();
        String databaseName = info.getProperty(LibSqlConstants.PROP_DATABASE_NAME);
        if (databaseName != null) {
            props.put(LibSqlConstants.PROP_DATABASE_NAME, databaseName);
        }
        if (url != null && !url.isEmpty()) {
            LibSqlUtils.parseUrlProperties(url, props);
        }
        DriverPropertyInfo database = new DriverPropertyInfo(
            LibSqlConstants.PROP_DATABASE_NAME, (String) props.get(LibSqlConstants.PROP_DATABASE_NAME));
        database.description = "Database namespace (optional; leave empty to use the server default)";
        return new DriverPropertyInfo[] { database };
    }

    @Override
    public int getMajorVersion() {
        return LibSqlConstants.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion() {
        return LibSqlConstants.DRIVER_VERSION_MINOR;
    }

    public int getMicroVersion() {
        return LibSqlConstants.DRIVER_VERSION_MICRO;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() {
        return parentLogger;
    }

    public String getDriverName() {
        return LibSqlConstants.DRIVER_NAME;
    }

    public String getFullVersion() {
        return getMajorVersion() + "." + getMinorVersion() + "." + getMicroVersion() +
               " (" + LibSqlConstants.DRIVER_INFO + ")";
    }
}
