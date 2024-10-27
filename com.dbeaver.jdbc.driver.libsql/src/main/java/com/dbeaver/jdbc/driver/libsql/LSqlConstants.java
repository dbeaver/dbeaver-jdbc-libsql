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

import java.util.regex.Pattern;

public class LSqlConstants {

    public static final Pattern CONNECTION_URL_EXAMPLE = Pattern.compile("jdbc:dbeaver:libsql:<server-url>");
    public static final Pattern CONNECTION_URL_PATTERN = Pattern.compile("jdbc:dbeaver:libsql:(.+)");
    public static final int DRIVER_VERSION_MAJOR = 1;
    public static final int DRIVER_VERSION_MINOR = 0;
    public static final String DRIVER_NAME = "LibSQL";
    public static final String DRIVER_INFO = "DBeaver LibSQL JDBC driver";
}
