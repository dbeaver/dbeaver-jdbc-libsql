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

import org.jkiss.utils.IOUtils;

import java.io.IOException;
import java.io.Reader;

public class LibSqlReaderInput {
    private Reader stream;
    private long length;

    public LibSqlReaderInput(Reader stream, long length) {
        this.stream = stream;
        this.length = length;
    }

    @Override
    public String toString() {
        try {
            String str = IOUtils.readToString(stream);
            if (length <= 0) {
                return str;
            }
            return str.substring(0, (int) length);
        } catch (IOException e) {
            return e.getMessage();
        }
    }
}
