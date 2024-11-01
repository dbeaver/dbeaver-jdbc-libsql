# LibSQL JDBC driver

[![status](https://img.shields.io/github/actions/workflow/status/dbeaver/dbeaver-jdbc-libsql/ci.yml?branch=devel)](https://github.com/dbeaver/dbeaver-jdbc-libsql/actions/workflows/ci.yml?query=branch%3Adevel)
[![javadoc](https://javadoc.io/badge2/dbeaver/dbeaver-jdbc-libsql/javadoc.svg)](https://javadoc.io/doc/dbeaver/dbeaver-jdbc-libsql)
[![Apache 2.0](https://img.shields.io/github/license/cronn-de/jira-sync.svg)](http://www.apache.org/licenses/LICENSE-2.0)

LibSQL [JDBC](https://en.wikipedia.org/wiki/JDBC_driver) is a library for accessing and creating [LibSQL](https://github.com/tursodatabase/libsql) database in Java.
- It is a pure Java library
- Version 1.0 uses simple [HTTP API](https://github.com/tursodatabase/libsql/blob/main/docs/http_api.md) protocol for LibSQL
- It supports prepared statements, database metadata, resultsets, data types and most of other JDBC features

## Download
Download from Maven Central or from the releases page.
```xml
<dependencies>
    <dependency>
      <groupId>com.dbeaver.jdbc</groupId>
      <artifactId>com.dbeaver.jdbc.driver.libsql</artifactId>
      <version>1.0.0</version>
    </dependency>
</dependencies>
```
