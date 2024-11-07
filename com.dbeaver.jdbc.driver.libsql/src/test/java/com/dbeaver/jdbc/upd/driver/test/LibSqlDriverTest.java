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
package com.dbeaver.jdbc.upd.driver.test;

import java.sql.*;

public class LibSqlDriverTest {
    /**
     * Runs simple select query in LibSQL
     *
     * @param args mvn exec:java "-Dexec.args=database-url [token]"
     */
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        try {
            String databaseUrl = args[0];
            String token = args.length > 1 ? args[1] : null;
            try (Connection connection = DriverManager.getConnection("jdbc:dbeaver:libsql:" + databaseUrl, null, token)) {
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
