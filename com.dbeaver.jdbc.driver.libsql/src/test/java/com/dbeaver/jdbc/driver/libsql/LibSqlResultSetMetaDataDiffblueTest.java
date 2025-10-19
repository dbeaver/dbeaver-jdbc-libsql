package com.dbeaver.jdbc.driver.libsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.sql.SQLException;
import java.util.ArrayList;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class LibSqlResultSetMetaDataDiffblueTest {
  /**
   * Test {@link LibSqlResultSetMetaData#getColumnCount()}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return zero.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getColumnCount()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSetMetaData.getColumnCount()"})
  public void testGetColumnCount_givenLibSqlStatementWithConnectionIsNull_thenReturnZero()
      throws SQLException {
    // Arrange
    LibSqlResultSet resultSet = new LibSqlResultSet(new LibSqlStatement(null), null);

    // Act and Assert
    assertEquals(0, new LibSqlResultSetMetaData(resultSet).getColumnCount());
  }

  /**
   * Test {@link LibSqlResultSetMetaData#isAutoIncrement(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code false}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#isAutoIncrement(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSetMetaData.isAutoIncrement(int)"})
  public void testIsAutoIncrement_givenLibSqlStatementWithConnectionIsNull_thenReturnFalse()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertFalse(new LibSqlResultSetMetaData(resultSet).isAutoIncrement(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#isCaseSensitive(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code false}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#isCaseSensitive(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSetMetaData.isCaseSensitive(int)"})
  public void testIsCaseSensitive_givenLibSqlStatementWithConnectionIsNull_thenReturnFalse()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertFalse(new LibSqlResultSetMetaData(resultSet).isCaseSensitive(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#isSearchable(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code true}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#isSearchable(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSetMetaData.isSearchable(int)"})
  public void testIsSearchable_givenLibSqlStatementWithConnectionIsNull_thenReturnTrue()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertTrue(new LibSqlResultSetMetaData(resultSet).isSearchable(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#isCurrency(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code false}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#isCurrency(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSetMetaData.isCurrency(int)"})
  public void testIsCurrency_givenLibSqlStatementWithConnectionIsNull_thenReturnFalse()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertFalse(new LibSqlResultSetMetaData(resultSet).isCurrency(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#isNullable(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return one.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#isNullable(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSetMetaData.isNullable(int)"})
  public void testIsNullable_givenLibSqlStatementWithConnectionIsNull_thenReturnOne()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertEquals(1, new LibSqlResultSetMetaData(resultSet).isNullable(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#isSigned(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code false}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#isSigned(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSetMetaData.isSigned(int)"})
  public void testIsSigned_givenLibSqlStatementWithConnectionIsNull_thenReturnFalse()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertFalse(new LibSqlResultSetMetaData(resultSet).isSigned(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getColumnDisplaySize(int)}.
   *
   * <ul>
   *   <li>Then return minus one.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getColumnDisplaySize(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSetMetaData.getColumnDisplaySize(int)"})
  public void testGetColumnDisplaySize_thenReturnMinusOne() throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertEquals(-1, new LibSqlResultSetMetaData(resultSet).getColumnDisplaySize(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getColumnName(int)}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When one.
   *   <li>Then return {@code foo}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getColumnName(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSetMetaData.getColumnName(int)"})
  public void testGetColumnName_givenArrayListAddFoo_whenOne_thenReturnFoo() throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");

    LibSqlExecutionResult result = mock(LibSqlExecutionResult.class);
    when(result.getColumns()).thenReturn(stringList);
    LibSqlResultSet resultSet = new LibSqlResultSet(new LibSqlStatement(null), result);

    // Act
    String actualColumnName = new LibSqlResultSetMetaData(resultSet).getColumnName(1);

    // Assert
    verify(result).getColumns();
    assertEquals("foo", actualColumnName);
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getColumnName(int)}.
   *
   * <ul>
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getColumnName(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSetMetaData.getColumnName(int)"})
  public void testGetColumnName_thenThrowSQLException() throws SQLException {
    // Arrange
    LibSqlExecutionResult result = mock(LibSqlExecutionResult.class);
    when(result.getColumns()).thenReturn(new ArrayList<>());
    LibSqlResultSet resultSet = new LibSqlResultSet(new LibSqlStatement(null), result);

    // Act and Assert
    assertThrows(SQLException.class, () -> new LibSqlResultSetMetaData(resultSet).getColumnName(1));
    verify(result).getColumns();
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getColumnName(int)}.
   *
   * <ul>
   *   <li>When zero.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getColumnName(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSetMetaData.getColumnName(int)"})
  public void testGetColumnName_whenZero_thenThrowSQLException() throws SQLException {
    // Arrange
    LibSqlExecutionResult result = mock(LibSqlExecutionResult.class);
    when(result.getColumns()).thenReturn(new ArrayList<>());
    LibSqlResultSet resultSet = new LibSqlResultSet(new LibSqlStatement(null), result);

    // Act and Assert
    assertThrows(SQLException.class, () -> new LibSqlResultSetMetaData(resultSet).getColumnName(0));
    verify(result).getColumns();
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getPrecision(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return minus one.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getPrecision(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSetMetaData.getPrecision(int)"})
  public void testGetPrecision_givenLibSqlStatementWithConnectionIsNull_thenReturnMinusOne()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertEquals(-1, new LibSqlResultSetMetaData(resultSet).getPrecision(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getScale(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return minus one.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getScale(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSetMetaData.getScale(int)"})
  public void testGetScale_givenLibSqlStatementWithConnectionIsNull_thenReturnMinusOne()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertEquals(-1, new LibSqlResultSetMetaData(resultSet).getScale(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getCatalogName(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code null}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getCatalogName(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSetMetaData.getCatalogName(int)"})
  public void testGetCatalogName_givenLibSqlStatementWithConnectionIsNull_thenReturnNull()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertNull(new LibSqlResultSetMetaData(resultSet).getCatalogName(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getSchemaName(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code null}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getSchemaName(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSetMetaData.getSchemaName(int)"})
  public void testGetSchemaName_givenLibSqlStatementWithConnectionIsNull_thenReturnNull()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertNull(new LibSqlResultSetMetaData(resultSet).getSchemaName(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getTableName(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code null}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getTableName(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSetMetaData.getTableName(int)"})
  public void testGetTableName_givenLibSqlStatementWithConnectionIsNull_thenReturnNull()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertNull(new LibSqlResultSetMetaData(resultSet).getTableName(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getColumnType(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return twelve.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getColumnType(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSetMetaData.getColumnType(int)"})
  public void testGetColumnType_givenLibSqlStatementWithConnectionIsNull_thenReturnTwelve()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertEquals(12, new LibSqlResultSetMetaData(resultSet).getColumnType(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getColumnType(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return twelve.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getColumnType(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSetMetaData.getColumnType(int)"})
  public void testGetColumnType_givenLibSqlStatementWithConnectionIsNull_thenReturnTwelve2()
      throws SQLException {
    // Arrange
    LibSqlResultSet resultSet = new LibSqlResultSet(new LibSqlStatement(null), null);

    // Act and Assert
    assertEquals(12, new LibSqlResultSetMetaData(resultSet).getColumnType(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getColumnTypeName(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code VARCHAR}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getColumnTypeName(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSetMetaData.getColumnTypeName(int)"})
  public void testGetColumnTypeName_givenLibSqlStatementWithConnectionIsNull_thenReturnVarchar()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertEquals("VARCHAR", new LibSqlResultSetMetaData(resultSet).getColumnTypeName(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#getColumnTypeName(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code VARCHAR}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#getColumnTypeName(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSetMetaData.getColumnTypeName(int)"})
  public void testGetColumnTypeName_givenLibSqlStatementWithConnectionIsNull_thenReturnVarchar2()
      throws SQLException {
    // Arrange
    LibSqlResultSet resultSet = new LibSqlResultSet(new LibSqlStatement(null), null);

    // Act and Assert
    assertEquals("VARCHAR", new LibSqlResultSetMetaData(resultSet).getColumnTypeName(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#isReadOnly(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code false}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#isReadOnly(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSetMetaData.isReadOnly(int)"})
  public void testIsReadOnly_givenLibSqlStatementWithConnectionIsNull_thenReturnFalse()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertFalse(new LibSqlResultSetMetaData(resultSet).isReadOnly(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#isWritable(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code true}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#isWritable(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSetMetaData.isWritable(int)"})
  public void testIsWritable_givenLibSqlStatementWithConnectionIsNull_thenReturnTrue()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertTrue(new LibSqlResultSetMetaData(resultSet).isWritable(1));
  }

  /**
   * Test {@link LibSqlResultSetMetaData#isDefinitelyWritable(int)}.
   *
   * <ul>
   *   <li>Given {@link LibSqlStatement#LibSqlStatement(LibSqlConnection)} with connection is {@code
   *       null}.
   *   <li>Then return {@code true}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSetMetaData#isDefinitelyWritable(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSetMetaData.isDefinitelyWritable(int)"})
  public void testIsDefinitelyWritable_givenLibSqlStatementWithConnectionIsNull_thenReturnTrue()
      throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet resultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertTrue(new LibSqlResultSetMetaData(resultSet).isDefinitelyWritable(1));
  }
}
