package com.dbeaver.jdbc.driver.libsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

public class LibSqlUtilsDiffblueTest {
  /**
   * Test {@link LibSqlUtils#quote(String)}.
   *
   * <p>Method under test: {@link LibSqlUtils#quote(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.quote(String)"})
  public void testQuote() {
    // Arrange, Act and Assert
    assertEquals("'42'", LibSqlUtils.quote("42"));
  }

  /**
   * Test {@link LibSqlUtils#unquote(String)}.
   *
   * <ul>
   *   <li>When {@code ```}.
   *   <li>Then return {@code `}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#unquote(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.unquote(String)"})
  public void testUnquote_whenBacktickBacktickBacktick_thenReturnBacktick() {
    // Arrange, Act and Assert
    assertEquals("`", LibSqlUtils.unquote("```"));
  }

  /**
   * Test {@link LibSqlUtils#unquote(String)}.
   *
   * <ul>
   *   <li>When {@code `}.
   *   <li>Then return {@code `}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#unquote(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.unquote(String)"})
  public void testUnquote_whenBacktick_thenReturnBacktick() {
    // Arrange, Act and Assert
    assertEquals("`", LibSqlUtils.unquote("`"));
  }

  /**
   * Test {@link LibSqlUtils#unquote(String)}.
   *
   * <ul>
   *   <li>When {@code [`]}.
   *   <li>Then return {@code `}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#unquote(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.unquote(String)"})
  public void testUnquote_whenLeftSquareBracketBacktickRightSquareBracket_thenReturnBacktick() {
    // Arrange, Act and Assert
    assertEquals("`", LibSqlUtils.unquote("[`]"));
  }

  /**
   * Test {@link LibSqlUtils#unquote(String)}.
   *
   * <ul>
   *   <li>When {@code Name}.
   *   <li>Then return {@code Name}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#unquote(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.unquote(String)"})
  public void testUnquote_whenName_thenReturnName() {
    // Arrange, Act and Assert
    assertEquals("Name", LibSqlUtils.unquote("Name"));
  }

  /**
   * Test {@link LibSqlUtils#unquote(String)}.
   *
   * <ul>
   *   <li>When {@code `Name}.
   *   <li>Then return {@code `Name}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#unquote(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.unquote(String)"})
  public void testUnquote_whenName_thenReturnName2() {
    // Arrange, Act and Assert
    assertEquals("`Name", LibSqlUtils.unquote("`Name"));
  }

  /**
   * Test {@link LibSqlUtils#unquote(String)}.
   *
   * <ul>
   *   <li>When {@code "Name}.
   *   <li>Then return {@code "Name}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#unquote(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.unquote(String)"})
  public void testUnquote_whenName_thenReturnName3() {
    // Arrange, Act and Assert
    assertEquals("\"Name", LibSqlUtils.unquote("\"Name"));
  }

  /**
   * Test {@link LibSqlUtils#unquote(String)}.
   *
   * <ul>
   *   <li>When {@code [Name}.
   *   <li>Then return {@code [Name}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#unquote(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.unquote(String)"})
  public void testUnquote_whenName_thenReturnName4() {
    // Arrange, Act and Assert
    assertEquals("[Name", LibSqlUtils.unquote("[Name"));
  }

  /**
   * Test {@link LibSqlUtils#unquote(String)}.
   *
   * <ul>
   *   <li>When {@code null}.
   *   <li>Then return {@code null}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#unquote(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.unquote(String)"})
  public void testUnquote_whenNull_thenReturnNull() {
    // Arrange, Act and Assert
    assertNull(LibSqlUtils.unquote(null));
  }

  /**
   * Test {@link LibSqlUtils#unquote(String)}.
   *
   * <ul>
   *   <li>When {@code "`"}.
   *   <li>Then return {@code `}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#unquote(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.unquote(String)"})
  public void testUnquote_whenQuotationMarkBacktickQuotationMark_thenReturnBacktick() {
    // Arrange, Act and Assert
    assertEquals("`", LibSqlUtils.unquote("\"`\""));
  }

  /**
   * Test {@link LibSqlUtils#escape(String)}.
   *
   * <p>Method under test: {@link LibSqlUtils#escape(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.escape(String)"})
  public void testEscape() {
    // Arrange, Act and Assert
    assertEquals("Val", LibSqlUtils.escape("Val"));
  }

  /**
   * Test {@link LibSqlUtils#executeQuery(Connection, String)}.
   *
   * <ul>
   *   <li>Given {@link SQLException#SQLException()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#executeQuery(Connection, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlUtils.executeQuery(Connection, String)"})
  public void testExecuteQuery_givenSQLException() throws SQLException {
    // Arrange
    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenThrow(new SQLException());

    // Act and Assert
    assertThrows(SQLException.class, () -> LibSqlUtils.executeQuery(connection, "Query"));
    verify(connection).createStatement();
  }

  /**
   * Test {@link LibSqlUtils#executeQuery(Connection, String)}.
   *
   * <ul>
   *   <li>Given {@link Statement} {@link Statement#executeQuery(String)} throw {@link
   *       SQLException#SQLException()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#executeQuery(Connection, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlUtils.executeQuery(Connection, String)"})
  public void testExecuteQuery_givenStatementExecuteQueryThrowSQLException() throws SQLException {
    // Arrange
    Statement statement = mock(Statement.class);
    when(statement.executeQuery(Mockito.<String>any())).thenThrow(new SQLException());
    doThrow(new SQLException()).when(statement).close();

    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    // Act and Assert
    assertThrows(SQLException.class, () -> LibSqlUtils.executeQuery(connection, "Query"));
    verify(connection).createStatement();
    verify(statement).close();
    verify(statement).executeQuery("Query");
  }

  /**
   * Test {@link LibSqlUtils#executeQuery(Connection, String)}.
   *
   * <ul>
   *   <li>Then return {@link LibSqlResultSet}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#executeQuery(Connection, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"ResultSet LibSqlUtils.executeQuery(Connection, String)"})
  public void testExecuteQuery_thenReturnLibSqlResultSet() throws SQLException {
    // Arrange
    Statement statement = mock(Statement.class);
    LibSqlStatement statement2 = new LibSqlStatement(null);
    LibSqlResultSet libSqlResultSet = new LibSqlResultSet(statement2, new LibSqlExecutionResult());
    when(statement.executeQuery(Mockito.<String>any())).thenReturn(libSqlResultSet);
    doNothing().when(statement).close();

    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenReturn(statement);

    // Act
    ResultSet actualExecuteQueryResult = LibSqlUtils.executeQuery(connection, "Query");

    // Assert
    verify(connection).createStatement();
    verify(statement).close();
    verify(statement).executeQuery("Query");
    assertTrue(actualExecuteQueryResult instanceof LibSqlResultSet);
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code https://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnHttpsExampleOrgExample() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "https://example.org/example",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:https://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code https://https://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnHttpsHttpsExampleOrgExample()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "https://https://example.org/example",
        LibSqlUtils.validateAndFormatUrl("libsql://https://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code https://jdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnHttpsJdbcDbeaverLibsql() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "https://jdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl("libsql://jdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code https://jdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnHttpsJdbcDbeaverLibsqlU() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "https://jdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl("libsql://jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code jdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnJdbcDbeaverLibsql() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "jdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:jdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code jdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnJdbcDbeaverLibsqlU() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "jdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U42https://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnU42httpsExampleOrgExample()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U42https://example.org/example",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U42https://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U42jdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnU42jdbcDbeaverLibsql() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U42jdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U42jdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U42jdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnU42jdbcDbeaverLibsqlU() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U42jdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U42jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U"https://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUHttpsExampleOrgExample() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U\"https://example.org/example",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\"https://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U[https://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUHttpsExampleOrgExample2() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U[https://example.org/example",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U[https://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U]https://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUHttpsExampleOrgExample3() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U]https://example.org/example",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]https://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U`https://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUHttpsExampleOrgExample4() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U`https://example.org/example",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U`https://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U"jdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUJdbcDbeaverLibsql() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U\"jdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\"jdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U[jdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUJdbcDbeaverLibsql2() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U[jdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U[jdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U]jdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUJdbcDbeaverLibsql3() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U]jdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]jdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U`jdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUJdbcDbeaverLibsql4() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U`jdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U`jdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U"jdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUJdbcDbeaverLibsqlU() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U\"jdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\"jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U[jdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUJdbcDbeaverLibsqlU2() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U[jdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U[jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U]jdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUJdbcDbeaverLibsqlU3() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U]jdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code U`jdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUJdbcDbeaverLibsqlU4() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U`jdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U`jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code UUrlhttps://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUUrlhttpsExampleOrgExample()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "UUrlhttps://example.org/example",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrlhttps://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code UUrljdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUUrljdbcDbeaverLibsql() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "UUrljdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrljdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code UUrljdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUUrljdbcDbeaverLibsqlU() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "UUrljdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrljdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExample() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/example",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Uhttps://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/example"}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExample2() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/example\"",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Uhttps://example.org/example\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/example[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExample3() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/example[",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Uhttps://example.org/example["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/example]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExample4() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/example]",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Uhttps://example.org/example]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/example`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExample5() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/example`",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Uhttps://example.org/example`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/example42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExample42()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/example42",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Uhttps://example.org/example42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/exampleUrl}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExampleUrl()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/exampleUrl",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Uhttps://example.org/exampleUrl"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/examplehttps://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExamplehttpsExampleOrgExample()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/examplehttps://example.org/example",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Uhttps://example.org/examplehttps://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/examplejdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExamplejdbcDbeaverLibsql()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/examplejdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Uhttps://example.org/examplejdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/examplejdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExamplejdbcDbeaverLibsqlU()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/examplejdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Uhttps://example.org/examplejdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Uhttps://example.org/examplelibsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUhttpsExampleOrgExamplelibsql()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Uhttps://example.org/examplelibsql://",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Uhttps://example.org/examplelibsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsql() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:"}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsql2() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:\"",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsql3() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:[",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsql4() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:]",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsql5() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:`",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsql42() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:42",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:https://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlHttpsExampleOrgExample()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:https://example.org/example",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:https://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:jdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlJdbcDbeaverLibsql()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:jdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:jdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:jdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlJdbcDbeaverLibsqlU()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:jdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:libsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlLibsql() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:libsql://",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:libsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlU() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:U"}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlU2() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:U\"",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:U\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:U[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlU3() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:U[",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:U["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:U]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlU4() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:U]",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:U]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:U`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlU5() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:U`",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:U`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:U42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlU42() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:U42",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:U42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:UUrl}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlUUrl() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:UUrl",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:UUrl"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:Uhttps://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlUhttpsExampleOrgExample()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:Uhttps://example.org/example",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:Uhttps://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlUjdbcDbeaverLibsql()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlUjdbcDbeaverLibsqlU()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:Ulibsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlUlibsql()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:Ulibsql://",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:Ulibsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ujdbc:dbeaver:libsql:Url}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUjdbcDbeaverLibsqlUrl() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ujdbc:dbeaver:libsql:Url",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ujdbc:dbeaver:libsql:Url"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ulibsql://https://example.org/example}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUlibsqlHttpsExampleOrgExample()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ulibsql://https://example.org/example",
        LibSqlUtils.validateAndFormatUrl(
            "jdbc:dbeaver:libsql:Ulibsql://https://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ulibsql://jdbc:dbeaver:libsql:}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUlibsqlJdbcDbeaverLibsql() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ulibsql://jdbc:dbeaver:libsql:",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ulibsql://jdbc:dbeaver:libsql:"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ulibsql://jdbc:dbeaver:libsql:U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUlibsqlJdbcDbeaverLibsqlU()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ulibsql://jdbc:dbeaver:libsql:U",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ulibsql://jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>Then return {@code Ulibsql://libsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_thenReturnUlibsqlLibsql() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ulibsql://libsql://",
        LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ulibsql://libsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code https://example.org/example}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenHttpsExampleOrgExample_thenThrowLibSqlException()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertThrows(
        LibSqlException.class,
        () -> LibSqlUtils.validateAndFormatUrl("https://example.org/example"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:42}.
   *   <li>Then return {@code 42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsql42_thenReturn42()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("42", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:libsql://}.
   *   <li>Then return {@code https://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlLibsql_thenReturnHttps()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("https://", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:libsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U42Url}.
   *   <li>Then return {@code U42Url}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42Url_thenReturnU42Url()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U42Url", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U42Url"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U42}.
   *   <li>Then return {@code U42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42_thenReturnU42()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U42", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U42"}.
   *   <li>Then return {@code U42"}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42_thenReturnU422()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U42\"", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U42\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U42[}.
   *   <li>Then return {@code U42[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42_thenReturnU423()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U42[", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U42["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U42]}.
   *   <li>Then return {@code U42]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42_thenReturnU424()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U42]", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U42]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U42`}.
   *   <li>Then return {@code U42`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42_thenReturnU425()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U42`", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U42`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U"42}.
   *   <li>Then return {@code U"42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42_thenReturnU426()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U\"42", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\"42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U[42}.
   *   <li>Then return {@code U[42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42_thenReturnU427()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U[42", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U[42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U]42}.
   *   <li>Then return {@code U]42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42_thenReturnU428()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U]42", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U`42}.
   *   <li>Then return {@code U`42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42_thenReturnU429()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U`42", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U`42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U42libsql://}.
   *   <li>Then return {@code U42libsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU42libsql_thenReturnU42libsql()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U42libsql://", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U42libsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U4242}.
   *   <li>Then return {@code U4242}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU4242_thenReturnU4242()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U4242", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U4242"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U"libsql://}.
   *   <li>Then return {@code U"libsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlULibsql_thenReturnULibsql()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U\"libsql://", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\"libsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U[libsql://}.
   *   <li>Then return {@code U[libsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlULibsql_thenReturnULibsql2()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U[libsql://", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U[libsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U]libsql://}.
   *   <li>Then return {@code U]libsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlULibsql_thenReturnULibsql3()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U]libsql://", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]libsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U`libsql://}.
   *   <li>Then return {@code U`libsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlULibsql_thenReturnULibsql4()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "U`libsql://", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U`libsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:UUrl42}.
   *   <li>Then return {@code UUrl42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrl42_thenReturnUUrl42()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("UUrl42", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrl42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:UUrlUrl}.
   *   <li>Then return {@code UUrlUrl}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrlUrl_thenReturnUUrlUrl()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("UUrlUrl", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrlUrl"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:UUrl}.
   *   <li>Then return {@code UUrl}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrl_thenReturnUUrl()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("UUrl", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrl"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:UUrl"}.
   *   <li>Then return {@code UUrl"}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrl_thenReturnUUrl2()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("UUrl\"", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrl\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:UUrl[}.
   *   <li>Then return {@code UUrl[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrl_thenReturnUUrl3()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("UUrl[", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrl["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:UUrl]}.
   *   <li>Then return {@code UUrl]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrl_thenReturnUUrl4()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("UUrl]", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrl]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:UUrl`}.
   *   <li>Then return {@code UUrl`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrl_thenReturnUUrl5()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("UUrl`", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrl`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U"Url}.
   *   <li>Then return {@code U"Url}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrl_thenReturnUUrl6()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U\"Url", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\"Url"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U[Url}.
   *   <li>Then return {@code U[Url}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrl_thenReturnUUrl7()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U[Url", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U[Url"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U]Url}.
   *   <li>Then return {@code U]Url}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrl_thenReturnUUrl8()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U]Url", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]Url"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U`Url}.
   *   <li>Then return {@code U`Url}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrl_thenReturnUUrl9()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U`Url", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U`Url"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:UUrllibsql://}.
   *   <li>Then return {@code UUrllibsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUUrllibsql_thenReturnUUrllibsql()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "UUrllibsql://", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:UUrllibsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U}.
   *   <li>Then return {@code U}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U"}.
   *   <li>Then return {@code U"}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU2()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U\"", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U[}.
   *   <li>Then return {@code U[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU3()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U[", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U]}.
   *   <li>Then return {@code U]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU4()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U]", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U`}.
   *   <li>Then return {@code U`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU5()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U`", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U""}.
   *   <li>Then return {@code U""}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU6()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U\"\"", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\"\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U"[}.
   *   <li>Then return {@code U"[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU7()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U\"[", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\"["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U"]}.
   *   <li>Then return {@code U"]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU8()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U\"]", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\"]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U"`}.
   *   <li>Then return {@code U"`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU9()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U\"`", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U\"`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U["}.
   *   <li>Then return {@code U["}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU10()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U[\"", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U[\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U[[}.
   *   <li>Then return {@code U[[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU11()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U[[", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U[["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U[]}.
   *   <li>Then return {@code U[]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU12()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U[]", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U[]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U[`}.
   *   <li>Then return {@code U[`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU13()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U[`", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U[`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U]"}.
   *   <li>Then return {@code U]"}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU14()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U]\"", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U][}.
   *   <li>Then return {@code U][}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU15()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U][", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U]]}.
   *   <li>Then return {@code U]]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU16()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U]]", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U]`}.
   *   <li>Then return {@code U]`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU17()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U]`", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U]`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U`"}.
   *   <li>Then return {@code U`"}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU18()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U`\"", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U`\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U`[}.
   *   <li>Then return {@code U`[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU19()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U`[", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U`["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U`]}.
   *   <li>Then return {@code U`]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU20()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U`]", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U`]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U``}.
   *   <li>Then return {@code U``}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlU_thenReturnU21()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("U``", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:U``"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:Ulibsql://42}.
   *   <li>Then return {@code Ulibsql://42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUlibsql42_thenReturnUlibsql42()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ulibsql://42", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ulibsql://42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:Ulibsql://Url}.
   *   <li>Then return {@code Ulibsql://Url}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUlibsqlUrl_thenReturnUlibsqlUrl()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ulibsql://Url", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ulibsql://Url"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:Ulibsql://}.
   *   <li>Then return {@code Ulibsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUlibsql_thenReturnUlibsql()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("Ulibsql://", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ulibsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:Ulibsql://"}.
   *   <li>Then return {@code Ulibsql://"}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUlibsql_thenReturnUlibsql2()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ulibsql://\"", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ulibsql://\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:Ulibsql://[}.
   *   <li>Then return {@code Ulibsql://[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUlibsql_thenReturnUlibsql3()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ulibsql://[", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ulibsql://["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:Ulibsql://]}.
   *   <li>Then return {@code Ulibsql://]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUlibsql_thenReturnUlibsql4()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ulibsql://]", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ulibsql://]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:Ulibsql://`}.
   *   <li>Then return {@code Ulibsql://`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUlibsql_thenReturnUlibsql5()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals(
        "Ulibsql://`", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Ulibsql://`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:Url}.
   *   <li>Then return {@code Url}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsqlUrl_thenReturnUrl()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("Url", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:Url"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:`}.
   *   <li>Then return {@code `}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsql_thenReturnBacktick()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("`", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:`"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:[}.
   *   <li>Then return {@code [}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsql_thenReturnLeftSquareBracket()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("[", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:"}.
   *   <li>Then return {@code "}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsql_thenReturnQuotationMark()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("\"", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:]}.
   *   <li>Then return {@code ]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenJdbcDbeaverLibsql_thenReturnRightSquareBracket()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("]", LibSqlUtils.validateAndFormatUrl("jdbc:dbeaver:libsql:]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code libsql://42}.
   *   <li>Then return {@code https://42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenLibsql42_thenReturnHttps42() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("https://42", LibSqlUtils.validateAndFormatUrl("libsql://42"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code libsql://libsql://}.
   *   <li>Then return {@code https://libsql://}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenLibsqlLibsql_thenReturnHttpsLibsql()
      throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("https://libsql://", LibSqlUtils.validateAndFormatUrl("libsql://libsql://"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code libsql://Url}.
   *   <li>Then return {@code https://Url}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenLibsqlUrl_thenReturnHttpsUrl() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("https://Url", LibSqlUtils.validateAndFormatUrl("libsql://Url"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code libsql://"}.
   *   <li>Then return {@code https://"}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenLibsql_thenReturnHttps() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("https://\"", LibSqlUtils.validateAndFormatUrl("libsql://\""));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code libsql://[}.
   *   <li>Then return {@code https://[}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenLibsql_thenReturnHttps2() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("https://[", LibSqlUtils.validateAndFormatUrl("libsql://["));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code libsql://]}.
   *   <li>Then return {@code https://]}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenLibsql_thenReturnHttps3() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("https://]", LibSqlUtils.validateAndFormatUrl("libsql://]"));
  }

  /**
   * Test {@link LibSqlUtils#validateAndFormatUrl(String)}.
   *
   * <ul>
   *   <li>When {@code libsql://`}.
   *   <li>Then return {@code https://`}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlUtils#validateAndFormatUrl(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlUtils.validateAndFormatUrl(String)"})
  public void testValidateAndFormatUrl_whenLibsql_thenReturnHttps4() throws LibSqlException {
    // Arrange, Act and Assert
    assertEquals("https://`", LibSqlUtils.validateAndFormatUrl("libsql://`"));
  }
}
