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

import com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult;
import com.dbeaver.jdbc.model.AbstractJdbcStatement;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedHashMap;
import java.util.Map;

public class LibSqlStatement extends AbstractJdbcStatement<LibSqlConnection> {

    protected String queryText;
    protected Map<Object, Object> parameters = new LinkedHashMap<>();

    protected LibSqlExecutionResult executionResult;
    protected LibSqlResultSet resultSet;

    public LibSqlStatement(@NotNull LibSqlConnection connection) throws SQLException {
        super(connection);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        executionResult = connection.getClient().execute(sql, parameters);
        return getResultSet();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(queryText);
    }

    @Override
    protected boolean execute(@NotNull String sql, @Nullable int[] columnIndexes, @Nullable String[] columnNames, int autoGeneratedKeys) throws SQLException {
        executionResult = connection.getClient().execute(sql, parameters);
        return true;
    }

    @Override
    public boolean execute() throws SQLException {
        executionResult = connection.getClient().execute(queryText, parameters);
        return true;
    }

    @Override
    protected int executeUpdate(@NotNull String sql, @Nullable int[] columnIndexes, @Nullable String[] columnNames, int autoGeneratedKeys) throws SQLException {
        executionResult = connection.getClient().execute(sql, parameters);
        return (int) executionResult.getUpdateCount();
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        executionResult = connection.getClient().execute(sql, parameters);
        return executionResult.getUpdateCount();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        executionResult = connection.getClient().execute(sql, parameters);
        return executionResult.getUpdateCount();
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        executionResult = connection.getClient().execute(sql, parameters);
        return executionResult.getUpdateCount();
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        executionResult = connection.getClient().execute(sql, parameters);
        return executionResult.getUpdateCount();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        executionResult = connection.getClient().execute(queryText, parameters);
        return executionResult.getUpdateCount();
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
            if (executionResult == null) {
                throw new SQLException("No result set was returned from server");
            }
            resultSet = new LibSqlResultSet(this, executionResult);
        }
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (executionResult == null) {
            throw new LibSqlException("No update count before statement execute");
        }
        return (int) executionResult.getUpdateCount();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        if (executionResult == null) {
            throw new LibSqlException("No update count before statement execute");
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
