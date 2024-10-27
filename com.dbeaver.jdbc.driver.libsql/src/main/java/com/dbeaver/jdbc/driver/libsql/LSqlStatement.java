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
import com.dbeaver.jdbc.model.AbstractJdbcStatement;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class LSqlStatement extends AbstractJdbcStatement<LSqlConnection> {

    protected LSqlExecutionResult executionResult;
    protected LSqlResultSet resultSet;

    public LSqlStatement(@NotNull LSqlConnection connection) throws SQLException {
        super(connection);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        LSqlExecutionResult result = connection.getClient().execute(sql);
        return new LSqlResultSet(result);
    }

    @Override
    protected boolean execute(@NotNull String sql, @Nullable int[] columnIndexes, @Nullable String[] columnNames, int autoGeneratedKeys) throws SQLException {
        throw new LSqlException("Base execute is not supported");
    }

    @Override
    protected int executeUpdate(@NotNull String sql, @Nullable int[] columnIndexes, @Nullable String[] columnNames, int autoGeneratedKeys) throws SQLException {
        throw new LSqlException("Base executeUpdate is not supported");
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        if (resultSet == null) {
            resultSet = new LSqlResultSet(this, executionResult);
        }
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (executionResult == null) {
            throw new LSqlException("No update count before statement execute");
        }
        return (int) executionResult.getUpdateCount();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        if (executionResult == null) {
            throw new LSqlException("No update count before statement execute");
        }
        return executionResult.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }
    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

}
