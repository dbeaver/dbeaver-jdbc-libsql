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
import com.dbeaver.jdbc.model.AbstractJdbcResultSetMetaData;
import org.jkiss.code.NotNull;
import org.jkiss.utils.CommonUtils;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class LibSqlResultSetMetaData extends AbstractJdbcResultSetMetaData<LibSqlStatement> {

    // We have json data.
    // Thus we can distinguish strings, numbers and booleans
    private enum ResultColumnDataType {
        BOOLEAN,
        NUMBER,
        STRING,
    }

    @NotNull
    private final LibSqlResultSet resultSet;

    public LibSqlResultSetMetaData(@NotNull LibSqlResultSet resultSet) throws SQLException {
        super(resultSet.getStatement());
        this.resultSet = resultSet;
    }

    private ResultColumnDataType getDataTypeFromData(int column) {
        LibSqlExecutionResult result = resultSet.getResult();
        if (result != null && !CommonUtils.isEmpty(result.getRows())) {
            Object columnValue = result.getRows().get(0)[column - 1];
            if (columnValue instanceof Boolean) {
                return ResultColumnDataType.BOOLEAN;
            } else if (columnValue instanceof Number) {
                return ResultColumnDataType.NUMBER;
            }
        }
        return ResultColumnDataType.STRING;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return resultSet.getResult() == null ? 0 : resultSet.getResult().getColumns().size();
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
        List<String> columns = resultSet.getResult().getColumns();
        if (column < 1 || column > columns.size()) {
            throw new SQLException("Column index out of bounds: " + column + "/" + columns.size());
        }
        return columns.get(column - 1);
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
        switch (getDataTypeFromData(column)) {
            case BOOLEAN:
                return Types.BOOLEAN;
            case NUMBER:
                return Types.NUMERIC;
            case STRING:
                return Types.VARCHAR;
            default:
                return Types.OTHER;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        switch (getDataTypeFromData(column)) {
            case BOOLEAN:
                return "BOOLEAN";
            case NUMBER:
                return "NUMERIC";
            case STRING:
                return "VARCHAR";
            default:
                return "UNKNOWN";
        }
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
