package com.dbeaver.test.libsql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import java.util.UUID;

public class LibSqlTest {
    public static void main(String[] args) throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:dbeaver:libsql:" + args[0], null, "password")) {
            try (Statement statement = connection.createStatement()) {
                String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
                System.out.println("Test table: " + tableName);
                statement.execute("drop table if exists " + tableName);
                statement.execute("create table " + tableName + " (id integer, name string)");
                statement.execute("insert into " + tableName + " values(1, 'test one')");
                statement.execute("insert into " + tableName + " values(2, 'test two')");
                try (ResultSet rs = statement.executeQuery("select * from " + tableName)) {
                    while (rs.next()) {
                        System.out.println(rs.getInt("id") + " = " + rs.getString("name"));
                    }
                }
                statement.executeUpdate("drop table " + tableName);
            }
        }
    }
}


