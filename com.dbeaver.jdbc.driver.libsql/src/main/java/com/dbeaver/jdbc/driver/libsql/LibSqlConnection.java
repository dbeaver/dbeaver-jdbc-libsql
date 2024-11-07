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

import com.dbeaver.jdbc.driver.libsql.client.LibSqlClient;
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
    @NotNull
    private final LibSqlClient client;
    @NotNull
    private final String url;
    @NotNull
    private final Map<String, Object> driverProperties;
    private LibSqlDatabaseMetaData databaseMetaData;

    public LibSqlConnection(
        @NotNull LibSqlDriver driver,
        @NotNull String url,
        @NotNull Map<String, Object> driverProperties
    ) throws SQLException {
        this.driver = driver;
        this.url = url;
        this.driverProperties = driverProperties;

        try {
            String token = CommonUtils.toString(driverProperties.get("password"), null);
            this.client = new LibSqlClient(new URL(url), token);
        } catch (IOException e) {
            throw new SQLException(e);
        }
        try {
            // Verify connection
            LibSqlUtils.executeQuery(this, "SELECT 1");
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    /**
     * Obtain transport client
     */
    public LibSqlClient getClient() {
        String applicationName = getClientApplicationName();
        if (!CommonUtils.isEmpty(applicationName)) {
            client.setUserAgent(applicationName);
        }
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
        client.close();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        if (databaseMetaData == null) {
            databaseMetaData = new LibSqlDatabaseMetaData(this);
        }
        return databaseMetaData;
    }

}
