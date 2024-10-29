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

import java.sql.SQLException;

public class LibSqlException extends SQLException {
    public LibSqlException(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public LibSqlException(String reason, String SQLState) {
        super(reason, SQLState);
    }

    public LibSqlException(String reason) {
        super(reason);
    }

    public LibSqlException() {
    }

    public LibSqlException(Throwable cause) {
        super(cause);
    }

    public LibSqlException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public LibSqlException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
    }

    public LibSqlException(String reason, String sqlState, int vendorCode, Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
    }
}
