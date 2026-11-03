/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2026 DBeaver Corp and others
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
package com.dbeaver.jdbc.driver.libsql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.DriverPropertyInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LibSqlConnectionPropertiesTest {
    @Test
    public void extractsNamespaceWithoutChangingOtherUrlComponents() throws Exception {
        Map<String, Object> properties = new HashMap<>();
        Assertions.assertEquals(
            "https://server.example:8443/proxy/?option=a%2Bb&empty=#fragment",
            LibSqlUtils.parseUrlProperties(
                "libsql://server.example:8443/proxy/?option=a%2Bb&db_name=test%2Bdb&empty=#fragment", properties));
        Assertions.assertEquals("test+db", properties.get("db_name"));
    }

    @Test
    public void removesJdbcOnlyQueryWhenItIsTheOnlyParameter() throws Exception {
        Map<String, Object> properties = new HashMap<>();
        Assertions.assertEquals("http://localhost:8080", LibSqlUtils.parseUrlProperties(
            "jdbc:dbeaver:libsql:http://localhost:8080?db_name=testdb", properties));
        Assertions.assertEquals("testdb", properties.get("db_name"));
    }

    @Test
    public void preservesUrlsWithoutNamespace() throws Exception {
        for (String suffix : new String[] {"", "/", "?option=a%2Bb&empty=", "#db_name=testdb"}) {
            Map<String, Object> properties = new HashMap<>();
            Assertions.assertEquals("https://server.example" + suffix, LibSqlUtils.parseUrlProperties(
                "libsql://server.example" + suffix, properties));
            Assertions.assertTrue(properties.isEmpty());
        }
    }

    @Test
    public void explicitPropertiesOverrideAllUrlValuesIncludingWhenEmpty() throws Exception {
        for (String database : new String[] {"from-properties", ""}) {
            Map<String, Object> properties = new HashMap<>(Map.of("db_name", database));
            Assertions.assertEquals("https://server.example", LibSqlUtils.parseUrlProperties(
                "libsql://server.example?db_name=first&db_name=second", properties));
            Assertions.assertEquals(database, properties.get("db_name"));
        }
    }

    @Test
    public void acceptsEmptyAndEncodedNamespaceParameters() throws Exception {
        for (String parameter : new String[] {"db_name", "db_name=", "db%5Fname="}) {
            Map<String, Object> properties = new HashMap<>();
            Assertions.assertEquals("https://server.example", LibSqlUtils.parseUrlProperties(
                "libsql://server.example?" + parameter, properties));
            Assertions.assertEquals("", properties.get("db_name"));
        }
    }

    @Test
    public void rejectsMalformedEncoding() {
        Assertions.assertThrows(LibSqlException.class, () -> LibSqlUtils.parseUrlProperties(
            "libsql://server.example?db_name=invalid%XX", new HashMap<>()));
    }

    @Test
    public void advertisesOptionalDatabasePropertyWithEffectiveValue() throws Exception {
        LibSqlDriver driver = new LibSqlDriver();
        Properties properties = new Properties();
        DriverPropertyInfo database = driver.getPropertyInfo("libsql://server.example?db_name=from-url", properties)[0];
        Assertions.assertEquals("db_name", database.name);
        Assertions.assertEquals("from-url", database.value);
        Assertions.assertFalse(database.required);
        Assertions.assertNotNull(database.description);
        Assertions.assertTrue(properties.isEmpty());

        properties.setProperty("db_name", "from-properties");
        Assertions.assertEquals("from-properties",
            driver.getPropertyInfo("libsql://server.example?db_name=from-url", properties)[0].value);
        Assertions.assertNull(driver.getPropertyInfo(null, new Properties())[0].value);
    }
}
