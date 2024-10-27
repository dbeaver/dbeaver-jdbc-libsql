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

import com.dbeaver.jdbc.model.AbstractJdbcResultSetMetaData;
import org.jkiss.code.NotNull;

import java.sql.SQLException;
import java.sql.Types;

public class LSqlResultSetMetaData extends AbstractJdbcResultSetMetaData<LSqlStatement> {

    @NotNull
    private final LSqlResultSet resultSet;

    public LSqlResultSetMetaData(@NotNull LSqlResultSet resultSet) throws SQLException {
        super(resultSet.getStatement());
        this.resultSet = resultSet;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return resultSet.getResult().getColumns().size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return -1;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return resultSet.getResult().getColumns().get(column - 1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return resultSet.getResult().getColumns().get(column - 1);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return -1;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return -1;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return Types.VARCHAR;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return "VARCHAR";
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return true;
    }

}
