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
import com.dbeaver.jdbc.model.AbstractJdbcResultSet;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.utils.CommonUtils;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class LibSqlResultSet extends AbstractJdbcResultSet<LibSqlStatement, LibSqlResultSetMetaData> {

    @Nullable
    private final LibSqlExecutionResult result;
    private transient int cursor = 0;
    private transient boolean closed;
    private transient boolean wasNull;
    private transient Map<String, Integer> nameMap;

    public LibSqlResultSet(@NotNull LibSqlStatement statement, @Nullable LibSqlExecutionResult result) {
        super(statement, null);
        this.result = result;
    }

    @Nullable
    public LibSqlExecutionResult getResult() {
        return result;
    }

    private int getColumnIndex(String columnLabel) throws LibSqlException {
        if (nameMap == null) {
            nameMap = new HashMap<>();
            List<String> columns = result.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                nameMap.put(columns.get(i).toUpperCase(Locale.ENGLISH), i + 1);
            }
        }
        Integer index = nameMap.get(columnLabel.toUpperCase(Locale.ENGLISH));
        if (index == null) {
            throw new LibSqlException("Column '" + columnLabel + "' is not present in result set");
        }
        return index;
    }

    private Object[] getCurrentRow() throws LibSqlException {
        List<Object[]> rows = result.getRows();
        if (cursor < 1) {
            throw new LibSqlException("Fetch not started");
        }
        if (cursor > rows.size()) {
            throw new LibSqlException("Fetch ended");
        }
        return rows.get(cursor - 1);
    }

    @Override
    public boolean next() throws SQLException {
        return cursor++ < result.getRows().size();
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return CommonUtils.toString(getObject(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return CommonUtils.toBoolean(getObject(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (byte) CommonUtils.toInt(getObject(columnIndex));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short) CommonUtils.toInt(getObject(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return CommonUtils.toInt(getObject(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return CommonUtils.toLong(getObject(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return CommonUtils.toFloat(getObject(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return CommonUtils.toDouble(getObject(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        Object object = getObject(columnIndex);
        return object == null ? null :
            object instanceof BigDecimal ? (BigDecimal) object :
                object instanceof Long ? BigDecimal.valueOf((Long) object) : BigDecimal.valueOf(CommonUtils.toDouble(object));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        return object == null ? null : object.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        return object == null ? null : Date.valueOf(object.toString());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        return object == null ? null : Time.valueOf(object.toString());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object object = getObject(columnIndex);
        return object == null ? null : Timestamp.valueOf(object.toString());
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return CommonUtils.toString(getObject(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return CommonUtils.toBoolean(getObject(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return (byte) CommonUtils.toInt(getObject(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return (short) CommonUtils.toInt(getObject(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return CommonUtils.toInt(getObject(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return CommonUtils.toLong(getObject(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return CommonUtils.toFloat(getObject(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return CommonUtils.toDouble(getObject(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        Object object = getObject(columnLabel);
        return object == null ? null :
            object instanceof BigDecimal ? (BigDecimal) object :
                object instanceof Long ? BigDecimal.valueOf((Long) object) : BigDecimal.valueOf(CommonUtils.toDouble(object));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        Object object = getObject(columnLabel);
        return object == null ? null : object.toString().getBytes();
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        Object object = getObject(columnLabel);
        return object == null ? null : Date.valueOf(object.toString());
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        Object object = getObject(columnLabel);
        return object == null ? null : Time.valueOf(object.toString());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        Object object = getObject(columnLabel);
        return object == null ? null : Timestamp.valueOf(object.toString());
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        return null;
    }

    @Override
    public LibSqlResultSetMetaData getMetaData() throws SQLException {
        if (metadata == null) {
            metadata = new LibSqlResultSetMetaData(this);
        }
        return metadata;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Object[] currentRow = getCurrentRow();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new LibSqlException("Column index " + columnIndex + " is beyond range (1-" + currentRow.length + ")");
        }
        Object value = currentRow[columnIndex - 1];
        wasNull = (value == null);
        return value;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        int index = getColumnIndex(columnLabel);
        return getObject(index);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return getColumnIndex(columnLabel);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

}
