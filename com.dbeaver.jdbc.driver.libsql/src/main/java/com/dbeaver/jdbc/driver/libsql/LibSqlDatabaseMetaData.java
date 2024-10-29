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

import com.dbeaver.jdbc.model.AbstractJdbcDatabaseMetaData;
import org.jkiss.code.NotNull;
import org.jkiss.utils.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LibSqlDatabaseMetaData extends AbstractJdbcDatabaseMetaData<LibSqlConnection> {

    private static Pattern VERSION_PATTERN = Pattern.compile("(\\w+)\\s+([0-9.]+)\\s+(.+)");
    private String serverVersion;

    public LibSqlDatabaseMetaData(@NotNull LibSqlConnection connection) {
        super(connection, connection.getUrl());
    }

    private void readServerVersion() throws SQLException {
        if (serverVersion != null) {
            return;
        }
        try {
            HttpURLConnection con = connection.getClient().openConnection("version");
            try (InputStream is = con.getInputStream()) {
                serverVersion = IOUtils.readLine(is);
            }
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        readServerVersion();
        return serverVersion;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        readServerVersion();
        Matcher matcher = VERSION_PATTERN.matcher(serverVersion);
        if (matcher.matches()) {
            return matcher.group(2);
        }
        return serverVersion;
    }

    @Override
    public String getDriverName() throws SQLException {
        return connection.getDriver().getDriverName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return connection.getDriver().getFullVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return connection.getDriver().getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return connection.getDriver().getMinorVersion();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        try (PreparedStatement dbStat = connection.prepareStatement(
            "SELECT NULL as TABLE_CAT, NULL AS TABLE_SCHEM," +
                "name AS TABLE_NAME,type as TABLE_TYPE, " +
                "NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME " +
                "FROM sqlite_master WHERE type='table'")
        ) {
            return dbStat.executeQuery();
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        String tableName = tableNamePattern;
        try (PreparedStatement dbStat = connection.prepareStatement(
            "SELECT NULL as TABLE_CAT, NULL AS TABLE_SCHEM,'" + tableName + "' AS TABLE_NAME," +
                "name as COLUMN_NAME," + Types.VARCHAR + " AS DATA_TYPE, type AS TYPE_NAME, 0 AS COLUMN_SIZE," +
                "NULL AS REMARKS,cid AS ORDINAL_POSITION " +
                "FROM PRAGMA_TABLE_INFO('" + tableName + "')")
        ) {
            return dbStat.executeQuery();
        }
    }
}
