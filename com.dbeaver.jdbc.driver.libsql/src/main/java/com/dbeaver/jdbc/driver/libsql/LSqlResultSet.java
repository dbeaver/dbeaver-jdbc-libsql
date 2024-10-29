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
import com.dbeaver.jdbc.model.AbstractJdbcResultSet;
import org.jkiss.code.NotNull;
import org.jkiss.utils.CommonUtils;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class LSqlResultSet extends AbstractJdbcResultSet<LSqlStatement, LSqlResultSetMetaData> {

    @NotNull
    private final LSqlExecutionResult result;
    private transient int cursor = 0;
    private transient boolean closed;
    private transient boolean wasNull;
    private transient Map<String, Integer> nameMap;

    public LSqlResultSet(@NotNull LSqlStatement statement, @NotNull LSqlExecutionResult result) {
        super(statement, null);
        this.result = result;
    }

    @NotNull
    public LSqlExecutionResult getResult() {
        return result;
    }

    private int getColumnIndex(String columnLabel) throws LSqlException {
        if (nameMap == null) {
            nameMap = new HashMap<>();
            List<String> columns = result.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                nameMap.put(columns.get(i).toUpperCase(Locale.ENGLISH), i + 1);
            }
        }
        Integer index = nameMap.get(columnLabel.toUpperCase(Locale.ENGLISH));
        if (index == null) {
            throw new LSqlException("Column '" + columnLabel + "' is not present in result set");
        }
        return index;
    }

    private Object[] getCurrentRow() throws LSqlException {
        List<Object[]> rows = result.getRows();
        if (cursor < 1) {
            throw new LSqlException("Fetch not started");
        }
        if (cursor > rows.size()) {
            throw new LSqlException("Fetch ended");
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
        return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return new byte[0];
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return CommonUtils.toString(getObject(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return false;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return null;
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
    public LSqlResultSetMetaData getMetaData() throws SQLException {
        if (metadata == null) {
            metadata = new LSqlResultSetMetaData(this);
        }
        return metadata;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        Object[] currentRow = getCurrentRow();
        if (columnIndex < 1 || columnIndex > currentRow.length) {
            throw new LSqlException("Column index " + columnIndex + " is beyond range (1-" + currentRow.length + ")");
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
