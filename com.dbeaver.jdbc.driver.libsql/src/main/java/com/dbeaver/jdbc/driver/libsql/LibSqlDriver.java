/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp
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
package com.dbeaver.jdbc.driver.libsql;


import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;

public class LibSqlDriver implements Driver {

    static final java.util.logging.Logger parentLogger = java.util.logging.Logger.getLogger("com.dbeaver.jdbc.upd.driver.driver");

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Matcher matcher = LibSqlConstants.CONNECTION_URL_PATTERN.matcher(url);
        if (!matcher.matches()) {
            throw new LibSqlException(
                "Invalid connection URL: " + url +
                ".\nExpected URL format: " + LibSqlConstants.CONNECTION_URL_EXAMPLE);
        }
        String targetUrl = matcher.group(1);

        Map<String, Object> props = new LinkedHashMap<>();
        for (Enumeration<?> pne = info.propertyNames(); pne.hasMoreElements(); ) {
            String propName = pne.toString();
            props.put(propName, info.get(propName));
        }
        return new LibSqlConnection(this, targetUrl, props);
    }

    @Override
    public boolean acceptsURL(String url) {
        return LibSqlConstants.CONNECTION_URL_PATTERN.matcher(url).matches();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[] {

        };
    }

    @Override
    public int getMajorVersion() {
        return LibSqlConstants.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion() {
        return LibSqlConstants.DRIVER_VERSION_MINOR;
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
        return getMajorVersion() + "." + getMinorVersion() + " (" + LibSqlConstants.DRIVER_INFO + ")";
    }
}
