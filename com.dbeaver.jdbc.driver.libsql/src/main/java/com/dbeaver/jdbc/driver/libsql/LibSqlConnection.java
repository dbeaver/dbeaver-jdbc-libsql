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

import com.dbeaver.jdbc.driver.libsql.client.LSqlClient;
import com.dbeaver.jdbc.model.AbstractJdbcConnection;
import org.jkiss.code.NotNull;
import org.jkiss.utils.CommonUtils;

import java.io.IOException;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class LibSqlConnection extends AbstractJdbcConnection {

    @NotNull
    private final LibSqlDriver driver;
    private final LSqlClient client;
    @NotNull
    private String url;
    @NotNull
    private Map<String, Object> driverProperties;

    public LibSqlConnection(
        @NotNull LibSqlDriver driver,
        @NotNull String url,
        @NotNull Map<String, Object> driverProperties
    ) throws SQLException {
        this.driver = driver;
        this.url = url;
        this.driverProperties = driverProperties;

        try {
            String token = CommonUtils.toString(driverProperties.get("password"));
            this.client = new LSqlClient(new URL(url), token);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        // Verify connection
        getMetaData().getDatabaseProductName();
    }

    public LSqlClient getClient() {
        return client;
    }

    @NotNull
    public String getUrl() {
        return url;
    }

    @NotNull
    public Map<String, Object> getDriverProperties() {
        return driverProperties;
    }

    @NotNull
    public LibSqlDriver getDriver() {
        return driver;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new LibSqlStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatementImpl(sql);
    }

    @NotNull
    private LibSqlPreparedStatement prepareStatementImpl(String sql) throws SQLException {
        return new LibSqlPreparedStatement(this, sql);
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new LibSqlDatabaseMetaData(this);
    }

}
