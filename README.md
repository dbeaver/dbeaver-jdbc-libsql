# LibSQL JDBC driver

[![CI](https://github.com/dbeaver/dbeaver-jdbc-libsql/actions/workflows/push-pr-devel.yml/badge.svg)](https://github.com/dbeaver/dbeaver-jdbc-libsql/actions/workflows/push-pr-devel.yml)
[![javadoc](https://javadoc.io/badge2/com.dbeaver.jdbc/com.dbeaver.jdbc.driver.libsql/javadoc.svg)](https://javadoc.io/doc/com.dbeaver.jdbc/com.dbeaver.jdbc.driver.libsql)
[![Apache 2.0](https://img.shields.io/github/license/cronn-de/jira-sync.svg)](http://www.apache.org/licenses/LICENSE-2.0)

Turso LibSQL [JDBC](https://en.wikipedia.org/wiki/JDBC_driver) is a library for accessing and managing [LibSQL](https://github.com/tursodatabase/libsql) databases in Java.
- It is a pure Java library
- Version 1.0.x uses simple [HTTP API](https://github.com/tursodatabase/libsql/blob/main/docs/http_api.md) protocol for LibSQL
- It supports prepared statements, database metadata, resultsets, data types and most of other JDBC features
- It supports Turso and local LibSQL servers
- It is included in [DBeaver](https://github.com/dbeaver/dbeaver) and [CloudBeaver](https://github.com/dbeaver/cloudbeaver) as default LibSQL driver. However, it can be used in any other products/frameworks which rely on JDBC API

## Reqirements

- Java 17
- Maven

## Building

Clone `dbeaver-common` alongside this repository and run:

```sh
../dbeaver-common/mvnw -f aggregate/pom.xml package
```

The default build produces a plain driver JAR. To also build the standalone shaded JAR and `target/dist` distribution, run:

```sh
../dbeaver-common/mvnw -f aggregate/pom.xml package -Pfull-jar
```

On Windows, use `../dbeaver-common/mvnw.cmd`.

## Usage

JDBC URL format: 
- `jdbc:dbeaver:libsql:<server-url>` - classic JDBC form
- `libsql://<server-host>` - Turso format

Server URL is a full URL including schema and port. For example:
- `jdbc:dbeaver:libsql:http://localhost:1234`
- `jdbc:dbeaver:libsql:https://test-test.turso.io`
- `libsql://test-test.turso.io`

Token based authentication supported in version 1.0. Pass token value as password, leave the username empty.  

Driver class name: `com.dbeaver.jdbc.driver.libsql.LibSqlDriver`

### Database namespaces (multitenancy)

For a standalone `sqld` server with namespaces enabled, select an existing database using the optional
`db_name` connection property or URL parameter:

```java
Properties properties = new Properties();
properties.setProperty("db_name", "testdb");
properties.setProperty("password", token); // Omit for servers without authentication
Connection connection = DriverManager.getConnection("jdbc:dbeaver:libsql:http://localhost:8080", properties);
```

Equivalent URL: `jdbc:dbeaver:libsql:http://localhost:8080?db_name=testdb`.
The Turso URL form also accepts this parameter: `libsql://server.example?db_name=testdb`.
URL-encode database names when passing them as URL parameters. An explicit connection property takes precedence
over the URL parameter. The driver sends the UTF-8 database name as unpadded standard Base64 in the
`x-namespace-bin` HTTP header (supported by stable and newer sqld releases), not as SQL schema selection.
If `db_name` is omitted or empty, existing server-default/hostname-based routing is preserved.

In DBeaver, enter the server URL and the optional **Database** name in the LibSQL connection settings.
Leave Database empty when the database is already selected by the hostname (for example, a Turso Cloud URL).
Namespaces must already exist; the driver does not create them.

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
      <version>1.0.4</version>
    </dependency>
</dependencies>
```
