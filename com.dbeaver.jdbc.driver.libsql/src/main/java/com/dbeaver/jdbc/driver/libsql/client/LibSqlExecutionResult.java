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
package com.dbeaver.jdbc.driver.libsql.client;

import java.util.List;

public class LibSqlExecutionResult {

    private List<String> columns;
    private List<Object[]> rows;
    private long rows_read;
    private long rows_written;
    private double query_duration_ms;

    public List<String> getColumns() {
        return columns;
    }

    public List<Object[]> getRows() {
        return rows;
    }

    public long getUpdateCount() {
        return rows_written;
    }

    public long getRowsRead() {
        return rows_read;
    }

    public double getRowsWritten() {
        return rows_written;
    }

    public double getQueryDurationMs() {
        return query_duration_ms;
    }
}
