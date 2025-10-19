package com.dbeaver.jdbc.driver.libsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.dbeaver.jdbc.driver.libsql.client.LibSqlClient;
import com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

public class LibSqlStatementDiffblueTest {
  /**
   * Test {@link LibSqlStatement#executeQuery(String)} with {@code String}.
   *
   * <ul>
   *   <li>Then return {@link LibSqlResultSet}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#executeQuery(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlStatement.executeQuery(String)"})
  public void testExecuteQueryWithString_thenReturnLibSqlResultSet() throws SQLException {
    // Arrange
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.execute(Mockito.<String>any(), Mockito.<Map<Object, Object>>any()))
        .thenReturn(new LibSqlExecutionResult());

    LibSqlConnection connection = mock(LibSqlConnection.class);
    when(connection.getClient()).thenReturn(libSqlClient);
    LibSqlStatement libSqlStatement = new LibSqlStatement(connection);

    // Act
    ResultSet actualExecuteQueryResult = libSqlStatement.executeQuery("Sql");

    // Assert
    verify(connection).getClient();
    verify(libSqlClient).execute(eq("Sql"), isA(Map.class));
    assertTrue(actualExecuteQueryResult instanceof LibSqlResultSet);
    LibSqlExecutionResult libSqlExecutionResult = libSqlStatement.executionResult;
    assertNull(libSqlExecutionResult.getRows());
    assertNull(libSqlExecutionResult.getColumns());
    assertEquals(0, libSqlStatement.getUpdateCount());
    assertEquals(0.0d, libSqlExecutionResult.getQueryDurationMs(), 0.0);
    assertEquals(0.0d, libSqlExecutionResult.getRowsWritten(), 0.0);
    assertEquals(0L, libSqlStatement.getLargeUpdateCount());
    assertEquals(0L, libSqlExecutionResult.getRowsRead());
    assertEquals(0L, libSqlExecutionResult.getUpdateCount());
    LibSqlResultSet libSqlResultSet = libSqlStatement.resultSet;
    assertSame(libSqlResultSet, actualExecuteQueryResult);
    assertSame(libSqlResultSet, libSqlStatement.getResultSet());
  }

  /**
   * Test {@link LibSqlStatement#executeQuery(String)} with {@code String}.
   *
   * <ul>
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#executeQuery(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlStatement.executeQuery(String)"})
  public void testExecuteQueryWithString_thenThrowSQLException() throws SQLException {
    // Arrange
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.execute(Mockito.<String>any(), Mockito.<Map<Object, Object>>any()))
        .thenReturn(null);

    LibSqlConnection connection = mock(LibSqlConnection.class);
    when(connection.getClient()).thenReturn(libSqlClient);

    // Act and Assert
    assertThrows(SQLException.class, () -> new LibSqlStatement(connection).executeQuery("Sql"));
    verify(connection).getClient();
    verify(libSqlClient).execute(eq("Sql"), isA(Map.class));
  }

  /**
   * Test {@link LibSqlStatement#executeQuery()}.
   *
   * <ul>
   *   <li>Given {@link LibSqlClient} {@link LibSqlClient#execute(String, Map)} return {@code null}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#executeQuery()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlStatement.executeQuery()"})
  public void testExecuteQuery_givenLibSqlClientExecuteReturnNull_thenThrowSQLException()
      throws SQLException {
    // Arrange
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.execute(Mockito.<String>any(), Mockito.<Map<Object, Object>>any()))
        .thenReturn(null);

    LibSqlConnection connection = mock(LibSqlConnection.class);
    when(connection.getClient()).thenReturn(libSqlClient);

    // Act and Assert
    assertThrows(SQLException.class, () -> new LibSqlStatement(connection).executeQuery());
    verify(connection).getClient();
    verify(libSqlClient).execute(isNull(), isA(Map.class));
  }

  /**
   * Test {@link LibSqlStatement#executeQuery()}.
   *
   * <ul>
   *   <li>Then return {@link LibSqlResultSet}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#executeQuery()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlStatement.executeQuery()"})
  public void testExecuteQuery_thenReturnLibSqlResultSet() throws SQLException {
    // Arrange
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.execute(Mockito.<String>any(), Mockito.<Map<Object, Object>>any()))
        .thenReturn(new LibSqlExecutionResult());

    LibSqlConnection connection = mock(LibSqlConnection.class);
    when(connection.getClient()).thenReturn(libSqlClient);
    LibSqlStatement libSqlStatement = new LibSqlStatement(connection);

    // Act
    ResultSet actualExecuteQueryResult = libSqlStatement.executeQuery();

    // Assert
    verify(connection).getClient();
    verify(libSqlClient).execute(isNull(), isA(Map.class));
    assertTrue(actualExecuteQueryResult instanceof LibSqlResultSet);
    LibSqlExecutionResult libSqlExecutionResult = libSqlStatement.executionResult;
    assertNull(libSqlExecutionResult.getRows());
    assertNull(libSqlExecutionResult.getColumns());
    assertEquals(0, libSqlStatement.getUpdateCount());
    assertEquals(0.0d, libSqlExecutionResult.getQueryDurationMs(), 0.0);
    assertEquals(0.0d, libSqlExecutionResult.getRowsWritten(), 0.0);
    assertEquals(0L, libSqlStatement.getLargeUpdateCount());
    assertEquals(0L, libSqlExecutionResult.getRowsRead());
    assertEquals(0L, libSqlExecutionResult.getUpdateCount());
    LibSqlResultSet libSqlResultSet = libSqlStatement.resultSet;
    assertSame(libSqlResultSet, actualExecuteQueryResult);
    assertSame(libSqlResultSet, libSqlStatement.getResultSet());
  }

  /**
   * Test {@link LibSqlStatement#execute(String, int[], String[], int)} with {@code sql}, {@code
   * columnIndexes}, {@code columnNames}, {@code autoGeneratedKeys}.
   *
   * <p>Method under test: {@link LibSqlStatement#execute(String, int[], String[], int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlStatement.execute(String, int[], String[], int)"})
  public void testExecuteWithSqlColumnIndexesColumnNamesAutoGeneratedKeys() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = mock(LibSqlPreparedStatement.class);
    when(libSqlPreparedStatement.execute(
            Mockito.<String>any(), Mockito.<int[]>any(), Mockito.<String[]>any(), anyInt()))
        .thenReturn(true);

    // Act
    libSqlPreparedStatement.execute(
        "Sql", new int[] {1, -1, 1, -1}, new String[] {"Column Names"}, 1);

    // Assert
    verify(libSqlPreparedStatement)
        .execute(eq("Sql"), isA(int[].class), isA(String[].class), eq(1));
  }

  /**
   * Test {@link LibSqlStatement#execute(String, int[], String[], int)} with {@code sql}, {@code
   * columnIndexes}, {@code columnNames}, {@code autoGeneratedKeys}.
   *
   * <p>Method under test: {@link LibSqlStatement#execute(String, int[], String[], int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlStatement.execute(String, int[], String[], int)"})
  public void testExecuteWithSqlColumnIndexesColumnNamesAutoGeneratedKeys2() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = mock(LibSqlPreparedStatement.class);
    when(libSqlPreparedStatement.execute(
            Mockito.<String>any(), Mockito.<int[]>any(), Mockito.<String[]>any(), anyInt()))
        .thenReturn(false);

    // Act
    libSqlPreparedStatement.execute(
        "Sql", new int[] {1, -1, 1, -1}, new String[] {"Column Names"}, 1);

    // Assert
    verify(libSqlPreparedStatement)
        .execute(eq("Sql"), isA(int[].class), isA(String[].class), eq(1));
  }

  /**
   * Test {@link LibSqlStatement#execute()}.
   *
   * <ul>
   *   <li>Given {@link LibSqlPreparedStatement} {@link LibSqlPreparedStatement#execute()} return
   *       {@code false}.
   *   <li>Then calls {@link LibSqlPreparedStatement#execute()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#execute()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlStatement.execute()"})
  public void testExecute_givenLibSqlPreparedStatementExecuteReturnFalse_thenCallsExecute()
      throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = mock(LibSqlPreparedStatement.class);
    when(libSqlPreparedStatement.execute()).thenReturn(false);

    // Act
    libSqlPreparedStatement.execute();

    // Assert
    verify(libSqlPreparedStatement).execute();
  }

  /**
   * Test {@link LibSqlStatement#execute()}.
   *
   * <ul>
   *   <li>Given {@link LibSqlPreparedStatement} {@link LibSqlPreparedStatement#execute()} return
   *       {@code true}.
   *   <li>Then calls {@link LibSqlPreparedStatement#execute()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#execute()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlStatement.execute()"})
  public void testExecute_givenLibSqlPreparedStatementExecuteReturnTrue_thenCallsExecute()
      throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = mock(LibSqlPreparedStatement.class);
    when(libSqlPreparedStatement.execute()).thenReturn(true);

    // Act
    libSqlPreparedStatement.execute();

    // Assert
    verify(libSqlPreparedStatement).execute();
  }

  /**
   * Test {@link LibSqlStatement#executeUpdate(String, int[], String[], int)} with {@code sql},
   * {@code columnIndexes}, {@code columnNames}, {@code autoGeneratedKeys}.
   *
   * <p>Method under test: {@link LibSqlStatement#executeUpdate(String, int[], String[], int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlStatement.executeUpdate(String, int[], String[], int)"})
  public void testExecuteUpdateWithSqlColumnIndexesColumnNamesAutoGeneratedKeys()
      throws SQLException {
    // Arrange
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.execute(Mockito.<String>any(), Mockito.<Map<Object, Object>>any()))
        .thenReturn(new LibSqlExecutionResult());

    LibSqlConnection connection = mock(LibSqlConnection.class);
    when(connection.getClient()).thenReturn(libSqlClient);
    LibSqlStatement libSqlStatement = new LibSqlStatement(connection);

    // Act
    int actualExecuteUpdateResult =
        libSqlStatement.executeUpdate(
            "Sql", new int[] {1, -1, 1, -1}, new String[] {"Column Names"}, 1);

    // Assert
    verify(connection).getClient();
    verify(libSqlClient).execute(eq("Sql"), isA(Map.class));
    ResultSet resultSet = libSqlStatement.getResultSet();
    assertTrue(resultSet instanceof LibSqlResultSet);
    LibSqlExecutionResult libSqlExecutionResult = libSqlStatement.executionResult;
    assertNull(libSqlExecutionResult.getRows());
    assertNull(libSqlExecutionResult.getColumns());
    assertEquals(0, actualExecuteUpdateResult);
    assertEquals(0, libSqlStatement.getUpdateCount());
    assertEquals(0.0d, libSqlExecutionResult.getQueryDurationMs(), 0.0);
    assertEquals(0.0d, libSqlExecutionResult.getRowsWritten(), 0.0);
    assertEquals(0L, libSqlStatement.getLargeUpdateCount());
    assertEquals(0L, libSqlExecutionResult.getRowsRead());
    assertEquals(0L, libSqlExecutionResult.getUpdateCount());
    assertSame((LibSqlResultSet) resultSet, libSqlStatement.resultSet);
  }

  /**
   * Test {@link LibSqlStatement#executeLargeUpdate()}.
   *
   * <p>Method under test: {@link LibSqlStatement#executeLargeUpdate()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"long LibSqlStatement.executeLargeUpdate()"})
  public void testExecuteLargeUpdate() throws SQLException {
    // Arrange
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.execute(Mockito.<String>any(), Mockito.<Map<Object, Object>>any()))
        .thenReturn(new LibSqlExecutionResult());

    LibSqlConnection connection = mock(LibSqlConnection.class);
    when(connection.getClient()).thenReturn(libSqlClient);
    LibSqlStatement libSqlStatement = new LibSqlStatement(connection);

    // Act
    long actualExecuteLargeUpdateResult = libSqlStatement.executeLargeUpdate();

    // Assert
    verify(connection).getClient();
    verify(libSqlClient).execute(isNull(), isA(Map.class));
    ResultSet resultSet = libSqlStatement.getResultSet();
    assertTrue(resultSet instanceof LibSqlResultSet);
    LibSqlExecutionResult libSqlExecutionResult = libSqlStatement.executionResult;
    assertNull(libSqlExecutionResult.getRows());
    assertNull(libSqlExecutionResult.getColumns());
    assertEquals(0, libSqlStatement.getUpdateCount());
    assertEquals(0.0d, libSqlExecutionResult.getQueryDurationMs(), 0.0);
    assertEquals(0.0d, libSqlExecutionResult.getRowsWritten(), 0.0);
    assertEquals(0L, actualExecuteLargeUpdateResult);
    assertEquals(0L, libSqlStatement.getLargeUpdateCount());
    assertEquals(0L, libSqlExecutionResult.getRowsRead());
    assertEquals(0L, libSqlExecutionResult.getUpdateCount());
    assertSame((LibSqlResultSet) resultSet, libSqlStatement.resultSet);
  }

  /**
   * Test {@link LibSqlStatement#executeLargeUpdate(String)} with {@code sql}.
   *
   * <p>Method under test: {@link LibSqlStatement#executeLargeUpdate(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"long LibSqlStatement.executeLargeUpdate(String)"})
  public void testExecuteLargeUpdateWithSql() throws SQLException {
    // Arrange
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.execute(Mockito.<String>any(), Mockito.<Map<Object, Object>>any()))
        .thenReturn(new LibSqlExecutionResult());

    LibSqlConnection connection = mock(LibSqlConnection.class);
    when(connection.getClient()).thenReturn(libSqlClient);
    LibSqlStatement libSqlStatement = new LibSqlStatement(connection);

    // Act
    long actualExecuteLargeUpdateResult = libSqlStatement.executeLargeUpdate("Sql");

    // Assert
    verify(connection).getClient();
    verify(libSqlClient).execute(eq("Sql"), isA(Map.class));
    ResultSet resultSet = libSqlStatement.getResultSet();
    assertTrue(resultSet instanceof LibSqlResultSet);
    LibSqlExecutionResult libSqlExecutionResult = libSqlStatement.executionResult;
    assertNull(libSqlExecutionResult.getRows());
    assertNull(libSqlExecutionResult.getColumns());
    assertEquals(0, libSqlStatement.getUpdateCount());
    assertEquals(0.0d, libSqlExecutionResult.getQueryDurationMs(), 0.0);
    assertEquals(0.0d, libSqlExecutionResult.getRowsWritten(), 0.0);
    assertEquals(0L, actualExecuteLargeUpdateResult);
    assertEquals(0L, libSqlStatement.getLargeUpdateCount());
    assertEquals(0L, libSqlExecutionResult.getRowsRead());
    assertEquals(0L, libSqlExecutionResult.getUpdateCount());
    assertSame((LibSqlResultSet) resultSet, libSqlStatement.resultSet);
  }

  /**
   * Test {@link LibSqlStatement#executeLargeUpdate(String, int)} with {@code sql}, {@code
   * autoGeneratedKeys}.
   *
   * <p>Method under test: {@link LibSqlStatement#executeLargeUpdate(String, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"long LibSqlStatement.executeLargeUpdate(String, int)"})
  public void testExecuteLargeUpdateWithSqlAutoGeneratedKeys() throws SQLException {
    // Arrange
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.execute(Mockito.<String>any(), Mockito.<Map<Object, Object>>any()))
        .thenReturn(new LibSqlExecutionResult());

    LibSqlConnection connection = mock(LibSqlConnection.class);
    when(connection.getClient()).thenReturn(libSqlClient);
    LibSqlStatement libSqlStatement = new LibSqlStatement(connection);

    // Act
    long actualExecuteLargeUpdateResult = libSqlStatement.executeLargeUpdate("Sql", 1);

    // Assert
    verify(connection).getClient();
    verify(libSqlClient).execute(eq("Sql"), isA(Map.class));
    ResultSet resultSet = libSqlStatement.getResultSet();
    assertTrue(resultSet instanceof LibSqlResultSet);
    LibSqlExecutionResult libSqlExecutionResult = libSqlStatement.executionResult;
    assertNull(libSqlExecutionResult.getRows());
    assertNull(libSqlExecutionResult.getColumns());
    assertEquals(0, libSqlStatement.getUpdateCount());
    assertEquals(0.0d, libSqlExecutionResult.getQueryDurationMs(), 0.0);
    assertEquals(0.0d, libSqlExecutionResult.getRowsWritten(), 0.0);
    assertEquals(0L, actualExecuteLargeUpdateResult);
    assertEquals(0L, libSqlStatement.getLargeUpdateCount());
    assertEquals(0L, libSqlExecutionResult.getRowsRead());
    assertEquals(0L, libSqlExecutionResult.getUpdateCount());
    assertSame((LibSqlResultSet) resultSet, libSqlStatement.resultSet);
  }

  /**
   * Test {@link LibSqlStatement#executeLargeUpdate(String, int[])} with {@code sql}, {@code
   * columnIndexes}.
   *
   * <p>Method under test: {@link LibSqlStatement#executeLargeUpdate(String, int[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"long LibSqlStatement.executeLargeUpdate(String, int[])"})
  public void testExecuteLargeUpdateWithSqlColumnIndexes() throws SQLException {
    // Arrange
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.execute(Mockito.<String>any(), Mockito.<Map<Object, Object>>any()))
        .thenReturn(new LibSqlExecutionResult());

    LibSqlConnection connection = mock(LibSqlConnection.class);
    when(connection.getClient()).thenReturn(libSqlClient);
    LibSqlStatement libSqlStatement = new LibSqlStatement(connection);

    // Act
    long actualExecuteLargeUpdateResult =
        libSqlStatement.executeLargeUpdate("Sql", new int[] {1, -1, 1, -1});

    // Assert
    verify(connection).getClient();
    verify(libSqlClient).execute(eq("Sql"), isA(Map.class));
    ResultSet resultSet = libSqlStatement.getResultSet();
    assertTrue(resultSet instanceof LibSqlResultSet);
    LibSqlExecutionResult libSqlExecutionResult = libSqlStatement.executionResult;
    assertNull(libSqlExecutionResult.getRows());
    assertNull(libSqlExecutionResult.getColumns());
    assertEquals(0, libSqlStatement.getUpdateCount());
    assertEquals(0.0d, libSqlExecutionResult.getQueryDurationMs(), 0.0);
    assertEquals(0.0d, libSqlExecutionResult.getRowsWritten(), 0.0);
    assertEquals(0L, actualExecuteLargeUpdateResult);
    assertEquals(0L, libSqlStatement.getLargeUpdateCount());
    assertEquals(0L, libSqlExecutionResult.getRowsRead());
    assertEquals(0L, libSqlExecutionResult.getUpdateCount());
    assertSame((LibSqlResultSet) resultSet, libSqlStatement.resultSet);
  }

  /**
   * Test {@link LibSqlStatement#executeLargeUpdate(String, String[])} with {@code sql}, {@code
   * columnNames}.
   *
   * <p>Method under test: {@link LibSqlStatement#executeLargeUpdate(String, String[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"long LibSqlStatement.executeLargeUpdate(String, String[])"})
  public void testExecuteLargeUpdateWithSqlColumnNames() throws SQLException {
    // Arrange
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.execute(Mockito.<String>any(), Mockito.<Map<Object, Object>>any()))
        .thenReturn(new LibSqlExecutionResult());

    LibSqlConnection connection = mock(LibSqlConnection.class);
    when(connection.getClient()).thenReturn(libSqlClient);
    LibSqlStatement libSqlStatement = new LibSqlStatement(connection);

    // Act
    long actualExecuteLargeUpdateResult =
        libSqlStatement.executeLargeUpdate("Sql", new String[] {"Column Names"});

    // Assert
    verify(connection).getClient();
    verify(libSqlClient).execute(eq("Sql"), isA(Map.class));
    ResultSet resultSet = libSqlStatement.getResultSet();
    assertTrue(resultSet instanceof LibSqlResultSet);
    LibSqlExecutionResult libSqlExecutionResult = libSqlStatement.executionResult;
    assertNull(libSqlExecutionResult.getRows());
    assertNull(libSqlExecutionResult.getColumns());
    assertEquals(0, libSqlStatement.getUpdateCount());
    assertEquals(0.0d, libSqlExecutionResult.getQueryDurationMs(), 0.0);
    assertEquals(0.0d, libSqlExecutionResult.getRowsWritten(), 0.0);
    assertEquals(0L, actualExecuteLargeUpdateResult);
    assertEquals(0L, libSqlStatement.getLargeUpdateCount());
    assertEquals(0L, libSqlExecutionResult.getRowsRead());
    assertEquals(0L, libSqlExecutionResult.getUpdateCount());
    assertSame((LibSqlResultSet) resultSet, libSqlStatement.resultSet);
  }

  /**
   * Test {@link LibSqlStatement#cancel()}.
   *
   * <ul>
   *   <li>Then throw {@link SQLFeatureNotSupportedException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#cancel()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlStatement.cancel()"})
  public void testCancel_thenThrowSQLFeatureNotSupportedException() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(SQLFeatureNotSupportedException.class, () -> new LibSqlStatement(null).cancel());
  }

  /**
   * Test {@link LibSqlStatement#getResultSet()}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#getResultSet()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlStatement.getResultSet()"})
  public void testGetResultSet_givenLibSqlStatementWithConnectionIsNull_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(SQLException.class, () -> new LibSqlStatement(null).getResultSet());
  }

  /**
   * Test {@link LibSqlStatement#getUpdateCount()}.
   *
   * <ul>
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#getUpdateCount()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlStatement.getUpdateCount()"})
  public void testGetUpdateCount_thenThrowLibSqlException() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(LibSqlException.class, () -> new LibSqlStatement(null).getUpdateCount());
  }

  /**
   * Test {@link LibSqlStatement#getLargeUpdateCount()}.
   *
   * <ul>
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#getLargeUpdateCount()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"long LibSqlStatement.getLargeUpdateCount()"})
  public void testGetLargeUpdateCount_thenThrowLibSqlException() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(LibSqlException.class, () -> new LibSqlStatement(null).getLargeUpdateCount());
  }

  /**
   * Test {@link LibSqlStatement#getMoreResults(int)} with {@code int}.
   *
   * <ul>
   *   <li>Then calls {@link LibSqlPreparedStatement#getMoreResults(int)}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#getMoreResults(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlStatement.getMoreResults(int)"})
  public void testGetMoreResultsWithInt_thenCallsGetMoreResults() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = mock(LibSqlPreparedStatement.class);
    when(libSqlPreparedStatement.getMoreResults(anyInt())).thenReturn(true);

    // Act
    libSqlPreparedStatement.getMoreResults(1);

    // Assert
    verify(libSqlPreparedStatement).getMoreResults(1);
  }

  /**
   * Test {@link LibSqlStatement#getMoreResults(int)} with {@code int}.
   *
   * <ul>
   *   <li>Then return {@code false}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#getMoreResults(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlStatement.getMoreResults(int)"})
  public void testGetMoreResultsWithInt_thenReturnFalse() throws SQLException {
    // Arrange, Act and Assert
    assertFalse(new LibSqlStatement(null).getMoreResults(1));
  }

  /**
   * Test {@link LibSqlStatement#getFetchDirection()}.
   *
   * <ul>
   *   <li>Then return one thousand.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStatement#getFetchDirection()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlStatement.getFetchDirection()"})
  public void testGetFetchDirection_thenReturnOneThousand() throws SQLException {
    // Arrange, Act and Assert
    assertEquals(1000, new LibSqlStatement(null).getFetchDirection());
  }
}
