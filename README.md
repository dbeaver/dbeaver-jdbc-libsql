# LibSQL JDBC driver

[![CI](https://github.com/dbeaver/dbeaver-jdbc-libsql/actions/workflows/push-pr-devel.yml/badge.svg)](https://github.com/dbeaver/dbeaver-jdbc-libsql/actions/workflows/push-pr-devel.yml)
[![javadoc](https://javadoc.io/badge2/com.dbeaver.jdbc/com.dbeaver.jdbc.driver.libsql/javadoc.svg)](https://javadoc.io/doc/com.dbeaver.jdbc/com.dbeaver.jdbc.driver.libsql)
[![Apache 2.0](https://img.shields.io/github/license/cronn-de/jira-sync.svg)](http://www.apache.org/licenses/LICENSE-2.0)

Turso LibSQL [JDBC](https://en.wikipedia.org/wiki/JDBC_driver) is a library for accessing and managing [LibSQL](https://github.com/tursodatabase/libsql) databases in Java.
- It is a pure Java library
- Version 1.0 uses simple [HTTP API](https://github.com/tursodatabase/libsql/blob/main/docs/http_api.md) protocol for LibSQL
- It supports prepared statements, database metadata, resultsets, data types and most of other JDBC features
- It supports Turso and local LibSQL servers
- It is included in [DBeaver](https://github.com/dbeaver/dbeaver) and [CloudBeaver](https://github.com/dbeaver/cloudbeaver) as default LibSQL driver. However, it can be used in any other products/frameworks which rely on JDBC API

## Reqirements

- Java 17
- Maven

## Usage

JDBC URL format: `jdbc:dbeaver:libsql:<server-url>`  
Server URL is a full URL including schema and port. For example:
- `jdbc:dbeaver:libsql:http://localhost:1234`
- `jdbc:dbeaver:libsql:https://test-test.turso.io`

Token based authentication supported in version 1.0. Pass token value as password, leave the username empty.  

Driver class name: `com.dbeaver.jdbc.driver.libsql.LibSqlDriver`

## Example

```java
import java.sql.*;

public class LibSqlTest {
    public static void main(String[] args) throws Exception {
        String databaseUrl = "http://libsql-server.company.local:8080";
        String token = args[0];
        try (Connection connection = DriverManager.getConnection("jdbc:dbeaver:libsql:" + databaseUrl, null, token)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("drop table if exists test_table_1");
                statement.execute("create table test_table_1 (id integer, name string)");
                statement.execute("insert into test_table_1 values(1, 'test one')");
                statement.execute("insert into test_table_1 values(2, 'test two')");
                try (ResultSet rs = statement.executeQuery("select * from test_table_1")) {
                    while (rs.next()) {
                        System.out.println(rs.getInt("id") + " = " + rs.getString("name"));
                    }
                }
            }
        }
    }
}
```
## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Download
Download from Maven Central or from the releases page.
```xml
<dependencies>
    <dependency>
      <groupId>com.dbeaver.jdbc</groupId>
      <artifactId>com.dbeaver.jdbc.driver.libsql</artifactId>
      <version>1.0.2</version>
    </dependency>
</dependencies>
```
