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
package com.dbeaver.jdbc.upd.driver.test;

import com.dbeaver.jdbc.driver.libsql.LSqlDriver;

import java.sql.*;
import java.util.Properties;

public class LSqlDriverTest {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            LSqlDriver driver = new LSqlDriver();
            Properties props = new Properties();
            try (Connection connection = driver.connect("jdbc:dbeaver:libsql:" + args[0], props)) {
                DatabaseMetaData metaData = connection.getMetaData();
                System.out.println("Driver: " + metaData.getDriverName());
                System.out.println("Database: " + metaData.getDatabaseProductName() + " " + metaData.getDatabaseProductVersion());

                System.out.println("Query:");
                try (Statement dbStat = connection.createStatement()) {
                    try (ResultSet dbResult = dbStat.executeQuery("select * from testme")) {
                        printResultSet(dbResult);
                    }
                }
                try (Statement dbStat = connection.createStatement()) {
                    try (ResultSet dbResult = dbStat.executeQuery("select * from PRAGMA_TABLE_INFO('testme')")) {
                        printResultSet(dbResult);
                    }
                }

                System.out.println("Tables:");
                try (ResultSet tables = metaData.getTables(null, null, null, null)) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");
                        System.out.println("\t- " + tableName);
                        try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
                            while (columns.next()) {
                                System.out.println("\t\t- " + columns.getString("COLUMN_NAME") + " " + columns.getString("TYPE_NAME"));
                            }
                        }

                    }
                }

            }
        } finally {
            System.out.println("Finished (" + (System.currentTimeMillis() - startTime) + "ms)");
        }
    }

    private static void printResultSet(ResultSet dbResults) throws SQLException {
        ResultSetMetaData rsmd = dbResults.getMetaData();
        System.out.print("|");
        for (int i = 0 ; i < rsmd.getColumnCount(); i++) {
            System.out.print(rsmd.getColumnLabel(i + 1) + " " + rsmd.getColumnTypeName(i + 1) + "|");
        }
        System.out.println();

        while (dbResults.next()) {
            System.out.print("|");
            for (int i = 0 ; i < rsmd.getColumnCount(); i++) {
                Object value = dbResults.getObject(i + 1);
                System.out.print(value + "|");
            }
            System.out.println();
        }
    }
}
