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

import com.dbeaver.jdbc.driver.libsql.client.LSqlExecutionResult;
import com.dbeaver.jdbc.model.AbstractJdbcDatabaseMetaData;
import org.jkiss.code.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class LSqlDatabaseMetaData extends AbstractJdbcDatabaseMetaData<LSqlConnection> {

    private final LSqlExecutionResult metaData;

    public LSqlDatabaseMetaData(@NotNull LSqlConnection connection, @NotNull LSqlExecutionResult metaData) {
        super(connection, connection.getUrl());
        this.metaData = metaData;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
        throw new SQLFeatureNotSupportedException();
    }

}
