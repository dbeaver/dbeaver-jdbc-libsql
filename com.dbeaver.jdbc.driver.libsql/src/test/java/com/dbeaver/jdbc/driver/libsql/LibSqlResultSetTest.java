package com.dbeaver.jdbc.driver.libsql;

import com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult;
import com.google.gson.Gson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LibSqlResultSetTest {

    private final Gson gson = new Gson();

    @Test
    public void testGetBooleanTrue() throws Exception {
        LibSqlExecutionResult result = gson.fromJson(
            """
            {
                "columns": ["value"],
                "rows": [[1]]
            }
            """,
            LibSqlExecutionResult.class
        );

        LibSqlResultSet resultSet = new LibSqlResultSet(null, result);

        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.getBoolean(1));
    }

    @Test
    public void testGetBooleanFalse() throws Exception {
        LibSqlExecutionResult result = gson.fromJson(
            """
            {
                "columns": ["value"],
                "rows": [[0]]
            }
            """,
            LibSqlExecutionResult.class
        );

        LibSqlResultSet resultSet = new LibSqlResultSet(null, result);

        Assertions.assertTrue(resultSet.next());
        Assertions.assertFalse(resultSet.getBoolean(1));
    }
}