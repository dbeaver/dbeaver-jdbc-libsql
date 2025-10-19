package com.dbeaver.jdbc.driver.libsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.dbeaver.jdbc.driver.libsql.LibSqlDatabaseMetaData.ImportedKeyFinder;
import com.dbeaver.jdbc.driver.libsql.LibSqlDatabaseMetaData.ImportedKeyFinder.ForeignKey;
import com.dbeaver.jdbc.driver.libsql.LibSqlDatabaseMetaData.PrimaryKeyFinder;
import com.dbeaver.jdbc.driver.libsql.client.LibSqlClient;
import com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LibSqlDatabaseMetaDataDiffblueTest {
  @Mock private LibSqlConnection libSqlConnection;

  @InjectMocks private LibSqlDatabaseMetaData libSqlDatabaseMetaData;

  /**
   * Test {@link LibSqlDatabaseMetaData#getDatabaseProductName()}.
   *
   * <ul>
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getDatabaseProductName()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlDatabaseMetaData.getDatabaseProductName()"})
  public void testGetDatabaseProductName_thenThrowSQLException() throws IOException, SQLException {
    // Arrange
    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.openSimpleConnection(Mockito.<String>any())).thenThrow(new IOException());
    when(libSqlConnection.getClient()).thenReturn(libSqlClient);
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlDatabaseMetaData.getDatabaseProductName());
    verify(libSqlConnection).getClient();
    verify(libSqlClient).openSimpleConnection("version");
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getDatabaseProductVersion()}.
   *
   * <ul>
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getDatabaseProductVersion()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlDatabaseMetaData.getDatabaseProductVersion()"})
  public void testGetDatabaseProductVersion_thenThrowSQLException()
      throws IOException, SQLException {
    // Arrange
    LibSqlClient libSqlClient = mock(LibSqlClient.class);
    when(libSqlClient.openSimpleConnection(Mockito.<String>any())).thenThrow(new IOException());
    when(libSqlConnection.getClient()).thenReturn(libSqlClient);
    Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri();

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlDatabaseMetaData.getDatabaseProductVersion());
    verify(libSqlConnection).getClient();
    verify(libSqlClient).openSimpleConnection("version");
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getDriverName()}.
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getDriverName()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlDatabaseMetaData.getDriverName()"})
  public void testGetDriverName() {
    // Arrange
    LibSqlDriver libSqlDriver = mock(LibSqlDriver.class);
    when(libSqlDriver.getDriverName()).thenReturn("Driver Name");
    when(libSqlConnection.getDriver()).thenReturn(libSqlDriver);

    // Act
    String actualDriverName = libSqlDatabaseMetaData.getDriverName();

    // Assert
    verify(libSqlConnection).getDriver();
    verify(libSqlDriver).getDriverName();
    assertEquals("Driver Name", actualDriverName);
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getDriverVersion()}.
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getDriverVersion()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlDatabaseMetaData.getDriverVersion()"})
  public void testGetDriverVersion() {
    // Arrange
    LibSqlDriver libSqlDriver = mock(LibSqlDriver.class);
    when(libSqlDriver.getFullVersion()).thenReturn("1.0.2");
    when(libSqlConnection.getDriver()).thenReturn(libSqlDriver);

    // Act
    String actualDriverVersion = libSqlDatabaseMetaData.getDriverVersion();

    // Assert
    verify(libSqlConnection).getDriver();
    verify(libSqlDriver).getFullVersion();
    assertEquals("1.0.2", actualDriverVersion);
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getDriverMajorVersion()}.
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getDriverMajorVersion()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlDatabaseMetaData.getDriverMajorVersion()"})
  public void testGetDriverMajorVersion() {
    // Arrange
    LibSqlDriver libSqlDriver = mock(LibSqlDriver.class);
    when(libSqlDriver.getMajorVersion()).thenReturn(1);
    when(libSqlConnection.getDriver()).thenReturn(libSqlDriver);

    // Act
    int actualDriverMajorVersion = libSqlDatabaseMetaData.getDriverMajorVersion();

    // Assert
    verify(libSqlConnection).getDriver();
    verify(libSqlDriver).getMajorVersion();
    assertEquals(1, actualDriverMajorVersion);
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getDriverMinorVersion()}.
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getDriverMinorVersion()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlDatabaseMetaData.getDriverMinorVersion()"})
  public void testGetDriverMinorVersion() {
    // Arrange
    LibSqlDriver libSqlDriver = mock(LibSqlDriver.class);
    when(libSqlDriver.getMinorVersion()).thenReturn(1);
    when(libSqlConnection.getDriver()).thenReturn(libSqlDriver);

    // Act
    int actualDriverMinorVersion = libSqlDatabaseMetaData.getDriverMinorVersion();

    // Assert
    verify(libSqlConnection).getDriver();
    verify(libSqlDriver).getMinorVersion();
    assertEquals(1, actualDriverMinorVersion);
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getTables(String, String, String, String[])}.
   *
   * <ul>
   *   <li>Given {@link LibSqlConnection} {@link LibSqlConnection#prepareStatement(String)} throw
   *       {@link SQLException#SQLException()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getTables(String, String, String,
   * String[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "ResultSet LibSqlDatabaseMetaData.getTables(String, String, String, String[])"
  })
  public void testGetTables_givenLibSqlConnectionPrepareStatementThrowSQLException()
      throws SQLException {
    // Arrange
    when(libSqlConnection.prepareStatement(Mockito.<String>any())).thenThrow(new SQLException());

    // Act and Assert
    assertThrows(
        SQLException.class,
        () ->
            libSqlDatabaseMetaData.getTables("", "", "Table Name Pattern", new String[] {"Types"}));
    verify(libSqlConnection)
        .prepareStatement(
            "SELECT NULL as TABLE_CAT, NULL AS TABLE_SCHEM,name AS TABLE_NAME,type as TABLE_TYPE, NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME FROM sqlite_master WHERE type='table'");
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getTables(String, String, String, String[])}.
   *
   * <ul>
   *   <li>Given {@link LibSqlConnection}.
   *   <li>When {@code Catalog}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getTables(String, String, String,
   * String[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "ResultSet LibSqlDatabaseMetaData.getTables(String, String, String, String[])"
  })
  public void testGetTables_givenLibSqlConnection_whenCatalog_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLException.class,
        () ->
            libSqlDatabaseMetaData.getTables(
                "Catalog", "Schema Pattern", "Table Name Pattern", new String[] {"Types"}));
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getTables(String, String, String, String[])}.
   *
   * <ul>
   *   <li>Given {@link LibSqlConnection}.
   *   <li>When {@code null}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getTables(String, String, String,
   * String[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "ResultSet LibSqlDatabaseMetaData.getTables(String, String, String, String[])"
  })
  public void testGetTables_givenLibSqlConnection_whenNull_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLException.class,
        () ->
            libSqlDatabaseMetaData.getTables(
                null, "Schema Pattern", "Table Name Pattern", new String[] {"Types"}));
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getTables(String, String, String, String[])}.
   *
   * <ul>
   *   <li>Given {@link LibSqlConnection}.
   *   <li>When {@code Schema Pattern}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getTables(String, String, String,
   * String[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "ResultSet LibSqlDatabaseMetaData.getTables(String, String, String, String[])"
  })
  public void testGetTables_givenLibSqlConnection_whenSchemaPattern_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLException.class,
        () ->
            libSqlDatabaseMetaData.getTables(
                "", "Schema Pattern", "Table Name Pattern", new String[] {"Types"}));
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getTables(String, String, String, String[])}.
   *
   * <ul>
   *   <li>Given {@link PreparedStatement} {@link PreparedStatement#executeQuery()} throw {@link
   *       SQLException#SQLException()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getTables(String, String, String,
   * String[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "ResultSet LibSqlDatabaseMetaData.getTables(String, String, String, String[])"
  })
  public void testGetTables_givenPreparedStatementExecuteQueryThrowSQLException()
      throws SQLException {
    // Arrange
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(preparedStatement.executeQuery()).thenThrow(new SQLException());
    doThrow(new SQLException()).when(preparedStatement).close();
    when(libSqlConnection.prepareStatement(Mockito.<String>any())).thenReturn(preparedStatement);

    // Act and Assert
    assertThrows(
        SQLException.class,
        () ->
            libSqlDatabaseMetaData.getTables("", "", "Table Name Pattern", new String[] {"Types"}));
    verify(libSqlConnection)
        .prepareStatement(
            "SELECT NULL as TABLE_CAT, NULL AS TABLE_SCHEM,name AS TABLE_NAME,type as TABLE_TYPE, NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME FROM sqlite_master WHERE type='table'");
    verify(preparedStatement).executeQuery();
    verify(preparedStatement).close();
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getTables(String, String, String, String[])}.
   *
   * <ul>
   *   <li>Then return {@link LibSqlResultSet}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getTables(String, String, String,
   * String[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "ResultSet LibSqlDatabaseMetaData.getTables(String, String, String, String[])"
  })
  public void testGetTables_thenReturnLibSqlResultSet() throws SQLException {
    // Arrange
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    LibSqlStatement statement = new LibSqlStatement(libSqlConnection);
    LibSqlResultSet libSqlResultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());
    when(preparedStatement.executeQuery()).thenReturn(libSqlResultSet);
    doNothing().when(preparedStatement).close();
    when(libSqlConnection.prepareStatement(Mockito.<String>any())).thenReturn(preparedStatement);

    // Act
    ResultSet actualTables =
        libSqlDatabaseMetaData.getTables("", "", "Table Name Pattern", new String[] {"Types"});

    // Assert
    verify(libSqlConnection)
        .prepareStatement(
            "SELECT NULL as TABLE_CAT, NULL AS TABLE_SCHEM,name AS TABLE_NAME,type as TABLE_TYPE, NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME FROM sqlite_master WHERE type='table'");
    verify(preparedStatement).executeQuery();
    verify(preparedStatement).close();
    assertTrue(actualTables instanceof LibSqlResultSet);
    assertSame(libSqlResultSet, actualTables);
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlConnection} {@link LibSqlConnection#createStatement()} throw {@link
   *       SQLException#SQLException()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlDatabaseMetaData.getColumns(String, String, String, String)"})
  public void testGetColumns_givenLibSqlConnectionCreateStatementThrowSQLException()
      throws SQLException {
    // Arrange
    when(libSqlConnection.createStatement()).thenThrow(new SQLException());

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlDatabaseMetaData.getColumns("", "", "", "Column Name Pattern"));
    verify(libSqlConnection).createStatement();
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlConnection}.
   *   <li>When {@code Catalog}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlDatabaseMetaData.getColumns(String, String, String, String)"})
  public void testGetColumns_givenLibSqlConnection_whenCatalog_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLException.class,
        () ->
            libSqlDatabaseMetaData.getColumns(
                "Catalog", "Schema Pattern", "Table Name", "Column Name Pattern"));
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlConnection}.
   *   <li>When {@code null}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlDatabaseMetaData.getColumns(String, String, String, String)"})
  public void testGetColumns_givenLibSqlConnection_whenNull_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlDatabaseMetaData.getColumns(null, "Schema Pattern", "", "Column Name Pattern"));
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlConnection}.
   *   <li>When {@code Schema Pattern}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlDatabaseMetaData.getColumns(String, String, String, String)"})
  public void testGetColumns_givenLibSqlConnection_whenSchemaPattern_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlDatabaseMetaData.getColumns("", "Schema Pattern", "", "Column Name Pattern"));
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}.
   *
   * <ul>
   *   <li>Then return {@link LibSqlResultSet}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlDatabaseMetaData.getColumns(String, String, String, String)"})
  public void testGetColumns_thenReturnLibSqlResultSet() throws SQLException {
    // Arrange
    Statement statement = mock(Statement.class);
    LibSqlStatement statement2 = new LibSqlStatement(libSqlConnection);
    LibSqlResultSet libSqlResultSet = new LibSqlResultSet(statement2, new LibSqlExecutionResult());
    when(statement.executeQuery(Mockito.<String>any())).thenReturn(libSqlResultSet);
    doNothing().when(statement).close();
    when(libSqlConnection.createStatement()).thenReturn(statement);

    // Act
    ResultSet actualColumns = libSqlDatabaseMetaData.getColumns("", "", "", "Column Name Pattern");

    // Assert
    verify(libSqlConnection).createStatement();
    verify(statement).close();
    verify(statement)
        .executeQuery(
            "WITH all_tables AS (SELECT name AS tn FROM sqlite_master WHERE type = 'table') \nSELECT NULL as TABLE_CAT, NULL AS TABLE_SCHEM, at.tn as TABLE_NAME,\npti.name as COLUMN_NAME,12 AS DATA_TYPE,pti.type AS TYPE_NAME,0 AS COLUMN_SIZE,0 as COLUMN_SIZE,0 as BUFFER_LENGTH,0 as DECIMAL_DIGITS,\n1 as NULLABLE,NULL as REMARKS,NULL as COLUMN_DEF,0 as SQL_DATA_TYPE,0 as SQL_DATETIME_SUB,0 as CHAR_OCTET_LENGTH,pti.cid as ORDINAL_POSITION,'' as IS_NULLABLE,NULL as SCOPE_CATALOG,NULL as SCOPE_SCHEMA,NULL as SCOPE_TABLE,NULL as SOURCE_DATA_TYPE,'' as IS_AUTOINCREMENT,'' as IS_GENERATEDCOLUMN\nFROM all_tables at INNER JOIN pragma_table_info(at.tn) pti\nORDER BY TABLE_NAME");
    assertTrue(actualColumns instanceof LibSqlResultSet);
    assertSame(libSqlResultSet, actualColumns);
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}.
   *
   * <ul>
   *   <li>Then return {@link LibSqlResultSet}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlDatabaseMetaData.getColumns(String, String, String, String)"})
  public void testGetColumns_thenReturnLibSqlResultSet2() throws SQLException {
    // Arrange
    Statement statement = mock(Statement.class);
    LibSqlStatement statement2 = new LibSqlStatement(libSqlConnection);
    LibSqlResultSet libSqlResultSet = new LibSqlResultSet(statement2, new LibSqlExecutionResult());
    when(statement.executeQuery(Mockito.<String>any())).thenReturn(libSqlResultSet);
    doNothing().when(statement).close();
    when(libSqlConnection.createStatement()).thenReturn(statement);

    // Act
    ResultSet actualColumns =
        libSqlDatabaseMetaData.getColumns("", "", "Table Name", "Column Name Pattern");

    // Assert
    verify(libSqlConnection).createStatement();
    verify(statement).close();
    verify(statement)
        .executeQuery(
            "WITH all_tables AS (SELECT name AS tn FROM sqlite_master WHERE type = 'table' and name='Table Name') \nSELECT NULL as TABLE_CAT, NULL AS TABLE_SCHEM, at.tn as TABLE_NAME,\npti.name as COLUMN_NAME,12 AS DATA_TYPE,pti.type AS TYPE_NAME,0 AS COLUMN_SIZE,0 as COLUMN_SIZE,0 as BUFFER_LENGTH,0 as DECIMAL_DIGITS,\n1 as NULLABLE,NULL as REMARKS,NULL as COLUMN_DEF,0 as SQL_DATA_TYPE,0 as SQL_DATETIME_SUB,0 as CHAR_OCTET_LENGTH,pti.cid as ORDINAL_POSITION,'' as IS_NULLABLE,NULL as SCOPE_CATALOG,NULL as SCOPE_SCHEMA,NULL as SCOPE_TABLE,NULL as SOURCE_DATA_TYPE,'' as IS_AUTOINCREMENT,'' as IS_GENERATEDCOLUMN\nFROM all_tables at INNER JOIN pragma_table_info(at.tn) pti\nORDER BY TABLE_NAME");
    assertTrue(actualColumns instanceof LibSqlResultSet);
    assertSame(libSqlResultSet, actualColumns);
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}.
   *
   * <ul>
   *   <li>When {@code %}.
   *   <li>Then return {@link LibSqlResultSet}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getColumns(String, String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlDatabaseMetaData.getColumns(String, String, String, String)"})
  public void testGetColumns_whenPercentSign_thenReturnLibSqlResultSet() throws SQLException {
    // Arrange
    Statement statement = mock(Statement.class);
    LibSqlStatement statement2 = new LibSqlStatement(libSqlConnection);
    LibSqlResultSet libSqlResultSet = new LibSqlResultSet(statement2, new LibSqlExecutionResult());
    when(statement.executeQuery(Mockito.<String>any())).thenReturn(libSqlResultSet);
    doNothing().when(statement).close();
    when(libSqlConnection.createStatement()).thenReturn(statement);

    // Act
    ResultSet actualColumns = libSqlDatabaseMetaData.getColumns("", "", "%", "Column Name Pattern");

    // Assert
    verify(libSqlConnection).createStatement();
    verify(statement).close();
    verify(statement)
        .executeQuery(
            "WITH all_tables AS (SELECT name AS tn FROM sqlite_master WHERE type = 'table') \nSELECT NULL as TABLE_CAT, NULL AS TABLE_SCHEM, at.tn as TABLE_NAME,\npti.name as COLUMN_NAME,12 AS DATA_TYPE,pti.type AS TYPE_NAME,0 AS COLUMN_SIZE,0 as COLUMN_SIZE,0 as BUFFER_LENGTH,0 as DECIMAL_DIGITS,\n1 as NULLABLE,NULL as REMARKS,NULL as COLUMN_DEF,0 as SQL_DATA_TYPE,0 as SQL_DATETIME_SUB,0 as CHAR_OCTET_LENGTH,pti.cid as ORDINAL_POSITION,'' as IS_NULLABLE,NULL as SCOPE_CATALOG,NULL as SCOPE_SCHEMA,NULL as SCOPE_TABLE,NULL as SOURCE_DATA_TYPE,'' as IS_AUTOINCREMENT,'' as IS_GENERATEDCOLUMN\nFROM all_tables at INNER JOIN pragma_table_info(at.tn) pti\nORDER BY TABLE_NAME");
    assertTrue(actualColumns instanceof LibSqlResultSet);
    assertSame(libSqlResultSet, actualColumns);
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getPrimaryKeys(String, String, String)}.
   *
   * <ul>
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getPrimaryKeys(String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlDatabaseMetaData.getPrimaryKeys(String, String, String)"})
  public void testGetPrimaryKeys_thenThrowSQLException() throws SQLException {
    // Arrange
    when(libSqlConnection.createStatement()).thenThrow(new SQLException());

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlDatabaseMetaData.getPrimaryKeys("Catalog", "Schema", "Table Name"));
    verify(libSqlConnection).createStatement();
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getIndexInfo(String, String, String, boolean, boolean)}.
   *
   * <ul>
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getIndexInfo(String, String, String,
   * boolean, boolean)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "ResultSet LibSqlDatabaseMetaData.getIndexInfo(String, String, String, boolean, boolean)"
  })
  public void testGetIndexInfo_thenThrowSQLException() throws SQLException {
    // Arrange
    when(libSqlConnection.createStatement()).thenThrow(new SQLException());

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlDatabaseMetaData.getIndexInfo("Catalog", "Schema", "Table", true, true));
    verify(libSqlConnection).createStatement();
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getImportedKeys(String, String, String)}.
   *
   * <ul>
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getImportedKeys(String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlDatabaseMetaData.getImportedKeys(String, String, String)"})
  public void testGetImportedKeys_thenThrowSQLException() throws SQLException {
    // Arrange
    when(libSqlConnection.createStatement()).thenThrow(new SQLException());

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlDatabaseMetaData.getImportedKeys("Catalog", "Schema", "Table"));
    verify(libSqlConnection).createStatement();
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getExportedKeys(String, String, String)}.
   *
   * <ul>
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getExportedKeys(String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlDatabaseMetaData.getExportedKeys(String, String, String)"})
  public void testGetExportedKeys_thenThrowSQLException() throws SQLException {
    // Arrange
    when(libSqlConnection.createStatement()).thenThrow(new SQLException());

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlDatabaseMetaData.getExportedKeys("Catalog", "Schema", "Table"));
    verify(libSqlConnection).createStatement();
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getCrossReference(String, String, String, String, String,
   * String)}.
   *
   * <ul>
   *   <li>Then return {@link LibSqlResultSet}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getCrossReference(String, String, String,
   * String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "ResultSet LibSqlDatabaseMetaData.getCrossReference(String, String, String, String, String, String)"
  })
  public void testGetCrossReference_thenReturnLibSqlResultSet() throws SQLException {
    // Arrange
    Statement statement = mock(Statement.class);
    LibSqlStatement statement2 = new LibSqlStatement(libSqlConnection);
    LibSqlResultSet libSqlResultSet = new LibSqlResultSet(statement2, new LibSqlExecutionResult());
    when(statement.executeQuery(Mockito.<String>any())).thenReturn(libSqlResultSet);
    doNothing().when(statement).close();
    when(libSqlConnection.createStatement()).thenReturn(statement);

    // Act
    ResultSet actualCrossReference =
        libSqlDatabaseMetaData.getCrossReference(
            "Parent Catalog",
            "Parent Schema",
            "Parent Table",
            "Foreign Catalog",
            "Foreign Schema",
            "Foreign Table");

    // Assert
    verify(libSqlConnection).createStatement();
    verify(statement).close();
    verify(statement)
        .executeQuery(
            "select 'Parent Catalog' as PKTABLE_CAT, 'Parent Schema' as PKTABLE_SCHEM, 'Parent Table' as PKTABLE_NAME, '' as PKCOLUMN_NAME, 'Foreign Catalog' as FKTABLE_CAT, 'Foreign Schema' as FKTABLE_SCHEM, 'Foreign Table' as FKTABLE_NAME, '' as FKCOLUMN_NAME, -1 as KEY_SEQ, 3 as UPDATE_RULE, 3 as DELETE_RULE, '' as FK_NAME, '' as PK_NAME, 5 as DEFERRABILITY limit 0 ");
    assertTrue(actualCrossReference instanceof LibSqlResultSet);
    assertSame(libSqlResultSet, actualCrossReference);
  }

  /**
   * Test {@link LibSqlDatabaseMetaData#getCrossReference(String, String, String, String, String,
   * String)}.
   *
   * <ul>
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDatabaseMetaData#getCrossReference(String, String, String,
   * String, String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "ResultSet LibSqlDatabaseMetaData.getCrossReference(String, String, String, String, String, String)"
  })
  public void testGetCrossReference_thenThrowSQLException() throws SQLException {
    // Arrange
    when(libSqlConnection.createStatement()).thenThrow(new SQLException());

    // Act and Assert
    assertThrows(
        SQLException.class,
        () ->
            libSqlDatabaseMetaData.getCrossReference(
                "Parent Catalog",
                "Parent Schema",
                "Parent Table",
                "Foreign Catalog",
                "Foreign Schema",
                "Foreign Table"));
    verify(libSqlConnection).createStatement();
  }

  /**
   * Test ImportedKeyFinder {@link ImportedKeyFinder#ImportedKeyFinder(Connection, String)}.
   *
   * <ul>
   *   <li>When empty string.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link ImportedKeyFinder#ImportedKeyFinder(Connection, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void ImportedKeyFinder.<init>(Connection, String)"})
  public void testImportedKeyFinderNewImportedKeyFinder_whenEmptyString_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(SQLException.class, () -> new ImportedKeyFinder(mock(Connection.class), ""));
  }

  /**
   * Test ImportedKeyFinder {@link ImportedKeyFinder#ImportedKeyFinder(Connection, String)}.
   *
   * <ul>
   *   <li>When {@code null}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link ImportedKeyFinder#ImportedKeyFinder(Connection, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void ImportedKeyFinder.<init>(Connection, String)"})
  public void testImportedKeyFinderNewImportedKeyFinder_whenNull_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(SQLException.class, () -> new ImportedKeyFinder(mock(Connection.class), null));
  }

  /**
   * Test ImportedKeyFinder_ForeignKey getters and setters.
   *
   * <p>Methods under test:
   *
   * <ul>
   *   <li>{@link ForeignKey#ForeignKey(String, String, String, String, String, String)}
   *   <li>{@link ForeignKey#toString()}
   *   <li>{@link ForeignKey#getFkName()}
   * </ul>
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void ForeignKey.<init>(String, String, String, String, String, String)",
    "String ForeignKey.getFkName()",
    "String ForeignKey.toString()"
  })
  public void testImportedKeyFinder_ForeignKeyGettersAndSetters() {
    // Arrange and Act
    ForeignKey actualForeignKey =
        new ForeignKey(
            "Fk Name", "Pk Table Name", "Fk Table Name", "2020-03-01", "On Delete", "Match");
    String actualToStringResult = actualForeignKey.toString();

    // Assert
    assertEquals("Fk Name", actualForeignKey.getFkName());
    assertEquals(
        "ForeignKey [fkName=Fk Name, pkTableName=Pk Table Name, fkTableName=Fk Table Name, pkColNames=[],"
            + " fkColNames=[]]",
        actualToStringResult);
  }

  /**
   * Test PrimaryKeyFinder {@link PrimaryKeyFinder#PrimaryKeyFinder(Connection, String)}.
   *
   * <ul>
   *   <li>Then return {@link PrimaryKeyFinder#table} is {@code sqlite_master}.
   * </ul>
   *
   * <p>Method under test: {@link PrimaryKeyFinder#PrimaryKeyFinder(Connection, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void PrimaryKeyFinder.<init>(Connection, String)"})
  public void testPrimaryKeyFinderNewPrimaryKeyFinder_thenReturnTableIsSqliteMaster()
      throws SQLException {
    // Arrange and Act
    PrimaryKeyFinder actualPrimaryKeyFinder =
        new PrimaryKeyFinder(mock(Connection.class), "sqlite_master");

    // Assert
    assertEquals("sqlite_master", actualPrimaryKeyFinder.table);
    assertNull(actualPrimaryKeyFinder.getName());
    assertNull(actualPrimaryKeyFinder.getColumns());
  }

  /**
   * Test PrimaryKeyFinder {@link PrimaryKeyFinder#PrimaryKeyFinder(Connection, String)}.
   *
   * <ul>
   *   <li>Then return {@link PrimaryKeyFinder#table} is {@code sqlite_schema}.
   * </ul>
   *
   * <p>Method under test: {@link PrimaryKeyFinder#PrimaryKeyFinder(Connection, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void PrimaryKeyFinder.<init>(Connection, String)"})
  public void testPrimaryKeyFinderNewPrimaryKeyFinder_thenReturnTableIsSqliteSchema()
      throws SQLException {
    // Arrange and Act
    PrimaryKeyFinder actualPrimaryKeyFinder =
        new PrimaryKeyFinder(mock(Connection.class), "sqlite_schema");

    // Assert
    assertEquals("sqlite_schema", actualPrimaryKeyFinder.table);
    assertNull(actualPrimaryKeyFinder.getName());
    assertNull(actualPrimaryKeyFinder.getColumns());
  }

  /**
   * Test PrimaryKeyFinder {@link PrimaryKeyFinder#PrimaryKeyFinder(Connection, String)}.
   *
   * <ul>
   *   <li>When empty string.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link PrimaryKeyFinder#PrimaryKeyFinder(Connection, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void PrimaryKeyFinder.<init>(Connection, String)"})
  public void testPrimaryKeyFinderNewPrimaryKeyFinder_whenEmptyString_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(SQLException.class, () -> new PrimaryKeyFinder(mock(Connection.class), ""));
  }

  /**
   * Test PrimaryKeyFinder {@link PrimaryKeyFinder#PrimaryKeyFinder(Connection, String)}.
   *
   * <ul>
   *   <li>When {@code null}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link PrimaryKeyFinder#PrimaryKeyFinder(Connection, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void PrimaryKeyFinder.<init>(Connection, String)"})
  public void testPrimaryKeyFinderNewPrimaryKeyFinder_whenNull_thenThrowSQLException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(SQLException.class, () -> new PrimaryKeyFinder(mock(Connection.class), null));
  }
}
