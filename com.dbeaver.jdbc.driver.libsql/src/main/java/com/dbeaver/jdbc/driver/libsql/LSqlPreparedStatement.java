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

import com.dbeaver.jdbc.driver.libsql.client.LSqlReaderInput;
import com.dbeaver.jdbc.driver.libsql.client.LSqlStreamInput;
import org.jkiss.code.NotNull;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

public class LSqlPreparedStatement extends LSqlStatement implements PreparedStatement {

    public LSqlPreparedStatement(
        @NotNull LSqlConnection connection, String sql) throws SQLException {
        super(connection);
        this.queryText = sql;
    }

    protected void addParameter(int parameterIndex, Object value) {
        this.parameters.put(parameterIndex, value);
    }

    protected void addParameter(String parameterName, Object value) {
        this.parameters.put(parameterName, value);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.addParameter(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlStreamInput(x, length));
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlStreamInput(x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlStreamInput(x, length));
    }

    @Override
    public void clearParameters() throws SQLException {
        this.parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batches not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlReaderInput(reader, length));
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement resultset metadata not supported");
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.addParameter(parameterIndex, null);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameters metadata not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        this.addParameter(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlReaderInput(value, length));
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.addParameter(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlReaderInput(reader, length));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlStreamInput(inputStream, length));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlReaderInput(reader, length));
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.addParameter(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.addParameter(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlStreamInput(x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlStreamInput(x, length));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.addParameter(parameterIndex, new LSqlReaderInput(reader, length));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        this.addParameter(parameterIndex, new LSqlStreamInput(x, -1));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        this.addParameter(parameterIndex, new LSqlStreamInput(x, -1));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        this.addParameter(parameterIndex, new LSqlReaderInput(reader, -1));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        this.addParameter(parameterIndex, new LSqlReaderInput(value, -1));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.addParameter(parameterIndex, new LSqlReaderInput(reader, -1));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.addParameter(parameterIndex, new LSqlStreamInput(inputStream, -1));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.addParameter(parameterIndex, new LSqlReaderInput(reader, -1));
    }
}
