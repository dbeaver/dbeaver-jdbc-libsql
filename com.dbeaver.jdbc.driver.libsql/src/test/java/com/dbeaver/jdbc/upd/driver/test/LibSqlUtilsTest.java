/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2025 DBeaver Corp
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
package com.dbeaver.jdbc.upd.driver.test;

import com.dbeaver.jdbc.driver.libsql.LibSqlException;
import com.dbeaver.jdbc.driver.libsql.LibSqlUtils;
import org.junit.Assert;
import org.junit.Test;

public class LibSqlUtilsTest {

    @Test
    public void testFormatUrl() throws LibSqlException {
        Assert.assertThrows(
            LibSqlException.class,
            () -> LibSqlUtils.validateAndFormatUrl("localhost")
        );

        assertUrlFormat("jdbc:dbeaver:libsql:http://localhost", "http://localhost");
        assertUrlFormat("jdbc:dbeaver:libsql:https://localhost", "https://localhost");
        assertUrlFormat("jdbc:dbeaver:libsql:http://localhost:8080", "http://localhost:8080");
        assertUrlFormat("jdbc:dbeaver:libsql:libsql://localhost", "https://localhost");
        assertUrlFormat("libsql://turso.url.my-hostname-1", "https://turso.url.my-hostname-1");
        assertUrlFormat("libsql://turso.url.my-hostname-1:8080", "https://turso.url.my-hostname-1:8080");
    }

    private void assertUrlFormat(String input, String expected) throws LibSqlException {
        Assert.assertEquals(expected, LibSqlUtils.validateAndFormatUrl(input));
    }
}
