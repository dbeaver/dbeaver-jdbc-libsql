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
