package com.dbeaver.jdbc.driver.libsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LibSqlResultSetDiffblueTest {
  @Mock private LibSqlExecutionResult libSqlExecutionResult;

  @InjectMocks private LibSqlResultSet libSqlResultSet;

  /**
   * Test {@link LibSqlResultSet#next()}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add array of {@link Object} with {@code 42}.
   *   <li>Then return {@code true}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#next()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSet.next()"})
  public void testNext_givenArrayListAddArrayOfObjectWith42_thenReturnTrue() throws SQLException {
    // Arrange
    ArrayList<Object[]> objectArrayList = new ArrayList<>();
    objectArrayList.add(new Object[] {"42"});
    when(libSqlExecutionResult.getRows()).thenReturn(objectArrayList);

    // Act
    boolean actualNextResult = libSqlResultSet.next();

    // Assert
    verify(libSqlExecutionResult).getRows();
    assertTrue(actualNextResult);
  }

  /**
   * Test {@link LibSqlResultSet#next()}.
   *
   * <ul>
   *   <li>Then return {@code false}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#next()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSet.next()"})
  public void testNext_thenReturnFalse() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act
    boolean actualNextResult = libSqlResultSet.next();

    // Assert
    verify(libSqlExecutionResult).getRows();
    assertFalse(actualNextResult);
  }

  /**
   * Test {@link LibSqlResultSet#getString(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getString(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSet.getString(int)"})
  public void testGetStringWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getString(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getString(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getString(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSet.getString(String)"})
  public void testGetStringWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getString("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getString(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getString(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSet.getString(String)"})
  public void testGetStringWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getString("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getString(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getString(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlResultSet.getString(String)"})
  public void testGetStringWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getString("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getBoolean(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getBoolean(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSet.getBoolean(int)"})
  public void testGetBooleanWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBoolean(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getBoolean(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBoolean(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSet.getBoolean(String)"})
  public void testGetBooleanWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBoolean("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getBoolean(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBoolean(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSet.getBoolean(String)"})
  public void testGetBooleanWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBoolean("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getBoolean(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBoolean(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlResultSet.getBoolean(String)"})
  public void testGetBooleanWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBoolean("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getByte(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getByte(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"byte LibSqlResultSet.getByte(int)"})
  public void testGetByteWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getByte(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getByte(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getByte(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"byte LibSqlResultSet.getByte(String)"})
  public void testGetByteWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getByte("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getByte(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getByte(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"byte LibSqlResultSet.getByte(String)"})
  public void testGetByteWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getByte("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getByte(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getByte(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"byte LibSqlResultSet.getByte(String)"})
  public void testGetByteWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getByte("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getShort(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getShort(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"short LibSqlResultSet.getShort(int)"})
  public void testGetShortWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getShort(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getShort(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getShort(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"short LibSqlResultSet.getShort(String)"})
  public void testGetShortWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getShort("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getShort(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getShort(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"short LibSqlResultSet.getShort(String)"})
  public void testGetShortWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getShort("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getShort(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getShort(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"short LibSqlResultSet.getShort(String)"})
  public void testGetShortWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getShort("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getInt(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getInt(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSet.getInt(int)"})
  public void testGetIntWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getInt(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getInt(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getInt(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSet.getInt(String)"})
  public void testGetIntWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getInt("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getInt(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getInt(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSet.getInt(String)"})
  public void testGetIntWithColumnLabel_givenArrayListAddFoo_whenColumnLabel() throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getInt("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getInt(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getInt(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSet.getInt(String)"})
  public void testGetIntWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getInt("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getLong(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getLong(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"long LibSqlResultSet.getLong(int)"})
  public void testGetLongWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getLong(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getLong(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getLong(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"long LibSqlResultSet.getLong(String)"})
  public void testGetLongWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getLong("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getLong(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getLong(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"long LibSqlResultSet.getLong(String)"})
  public void testGetLongWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getLong("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getLong(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getLong(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"long LibSqlResultSet.getLong(String)"})
  public void testGetLongWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getLong("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getFloat(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getFloat(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"float LibSqlResultSet.getFloat(int)"})
  public void testGetFloatWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getFloat(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getFloat(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getFloat(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"float LibSqlResultSet.getFloat(String)"})
  public void testGetFloatWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getFloat("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getFloat(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getFloat(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"float LibSqlResultSet.getFloat(String)"})
  public void testGetFloatWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getFloat("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getFloat(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getFloat(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"float LibSqlResultSet.getFloat(String)"})
  public void testGetFloatWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getFloat("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getDouble(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getDouble(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"double LibSqlResultSet.getDouble(int)"})
  public void testGetDoubleWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getDouble(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getDouble(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getDouble(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"double LibSqlResultSet.getDouble(String)"})
  public void testGetDoubleWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getDouble("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getDouble(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getDouble(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"double LibSqlResultSet.getDouble(String)"})
  public void testGetDoubleWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getDouble("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getDouble(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getDouble(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"double LibSqlResultSet.getDouble(String)"})
  public void testGetDoubleWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getDouble("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getBigDecimal(int, int)} with {@code columnIndex}, {@code scale}.
   *
   * <ul>
   *   <li>When minus one.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBigDecimal(int, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.math.BigDecimal LibSqlResultSet.getBigDecimal(int, int)"})
  public void testGetBigDecimalWithColumnIndexScale_whenMinusOne() throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet libSqlResultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBigDecimal(-1, 3));
  }

  /**
   * Test {@link LibSqlResultSet#getBigDecimal(int, int)} with {@code columnIndex}, {@code scale}.
   *
   * <ul>
   *   <li>When one.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBigDecimal(int, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.math.BigDecimal LibSqlResultSet.getBigDecimal(int, int)"})
  public void testGetBigDecimalWithColumnIndexScale_whenOne() throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet libSqlResultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBigDecimal(1, 3));
  }

  /**
   * Test {@link LibSqlResultSet#getBigDecimal(int, int)} with {@code columnIndex}, {@code scale}.
   *
   * <ul>
   *   <li>When three.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBigDecimal(int, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.math.BigDecimal LibSqlResultSet.getBigDecimal(int, int)"})
  public void testGetBigDecimalWithColumnIndexScale_whenThree() throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet libSqlResultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBigDecimal(3, 3));
  }

  /**
   * Test {@link LibSqlResultSet#getBigDecimal(int, int)} with {@code columnIndex}, {@code scale}.
   *
   * <ul>
   *   <li>When zero.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBigDecimal(int, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.math.BigDecimal LibSqlResultSet.getBigDecimal(int, int)"})
  public void testGetBigDecimalWithColumnIndexScale_whenZero() throws SQLException {
    // Arrange
    LibSqlStatement statement = new LibSqlStatement(null);
    LibSqlResultSet libSqlResultSet = new LibSqlResultSet(statement, new LibSqlExecutionResult());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBigDecimal(0, 3));
  }

  /**
   * Test {@link LibSqlResultSet#getBigDecimal(String, int)} with {@code columnLabel}, {@code
   * scale}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBigDecimal(String, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.math.BigDecimal LibSqlResultSet.getBigDecimal(String, int)"})
  public void testGetBigDecimalWithColumnLabelScale_givenArrayListAdd42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    stringList.add("foo");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBigDecimal("42", 1));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getBigDecimal(String, int)} with {@code columnLabel}, {@code
   * scale}.
   *
   * <ul>
   *   <li>When {@code 2.3}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBigDecimal(String, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.math.BigDecimal LibSqlResultSet.getBigDecimal(String, int)"})
  public void testGetBigDecimalWithColumnLabelScale_when23_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBigDecimal("2.3", 1));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getBigDecimal(String, int)} with {@code columnLabel}, {@code
   * scale}.
   *
   * <ul>
   *   <li>When {@code 2.3}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBigDecimal(String, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.math.BigDecimal LibSqlResultSet.getBigDecimal(String, int)"})
  public void testGetBigDecimalWithColumnLabelScale_when23_thenThrowLibSqlException2()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBigDecimal("2.3", 1));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getBytes(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getBytes(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"byte[] LibSqlResultSet.getBytes(int)"})
  public void testGetBytesWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBytes(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getBytes(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBytes(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"byte[] LibSqlResultSet.getBytes(String)"})
  public void testGetBytesWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBytes("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getBytes(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBytes(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"byte[] LibSqlResultSet.getBytes(String)"})
  public void testGetBytesWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBytes("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getBytes(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getBytes(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"byte[] LibSqlResultSet.getBytes(String)"})
  public void testGetBytesWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getBytes("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getDate(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getDate(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Date LibSqlResultSet.getDate(int)"})
  public void testGetDateWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getDate(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getDate(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getDate(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Date LibSqlResultSet.getDate(String)"})
  public void testGetDateWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getDate("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getDate(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getDate(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Date LibSqlResultSet.getDate(String)"})
  public void testGetDateWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getDate("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getDate(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getDate(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Date LibSqlResultSet.getDate(String)"})
  public void testGetDateWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getDate("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getTime(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getTime(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Time LibSqlResultSet.getTime(int)"})
  public void testGetTimeWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getTime(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getTime(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getTime(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Time LibSqlResultSet.getTime(String)"})
  public void testGetTimeWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getTime("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getTime(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getTime(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Time LibSqlResultSet.getTime(String)"})
  public void testGetTimeWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getTime("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getTime(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getTime(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Time LibSqlResultSet.getTime(String)"})
  public void testGetTimeWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getTime("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getTimestamp(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getTimestamp(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Timestamp LibSqlResultSet.getTimestamp(int)"})
  public void testGetTimestampWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getTimestamp(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getTimestamp(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getTimestamp(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Timestamp LibSqlResultSet.getTimestamp(String)"})
  public void testGetTimestampWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getTimestamp("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getTimestamp(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getTimestamp(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Timestamp LibSqlResultSet.getTimestamp(String)"})
  public void testGetTimestampWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getTimestamp("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getTimestamp(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getTimestamp(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.Timestamp LibSqlResultSet.getTimestamp(String)"})
  public void testGetTimestampWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getTimestamp("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getAsciiStream(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getAsciiStream(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.io.InputStream LibSqlResultSet.getAsciiStream(int)"})
  public void testGetAsciiStreamWithColumnIndex() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(SQLFeatureNotSupportedException.class, () -> libSqlResultSet.getAsciiStream(1));
  }

  /**
   * Test {@link LibSqlResultSet#getAsciiStream(String)} with {@code columnLabel}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getAsciiStream(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.io.InputStream LibSqlResultSet.getAsciiStream(String)"})
  public void testGetAsciiStreamWithColumnLabel() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> libSqlResultSet.getAsciiStream("Column Label"));
  }

  /**
   * Test {@link LibSqlResultSet#getUnicodeStream(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getUnicodeStream(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.io.InputStream LibSqlResultSet.getUnicodeStream(int)"})
  public void testGetUnicodeStreamWithColumnIndex() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(SQLFeatureNotSupportedException.class, () -> libSqlResultSet.getUnicodeStream(1));
  }

  /**
   * Test {@link LibSqlResultSet#getUnicodeStream(String)} with {@code columnLabel}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getUnicodeStream(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.io.InputStream LibSqlResultSet.getUnicodeStream(String)"})
  public void testGetUnicodeStreamWithColumnLabel() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> libSqlResultSet.getUnicodeStream("Column Label"));
  }

  /**
   * Test {@link LibSqlResultSet#getBinaryStream(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getBinaryStream(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.io.InputStream LibSqlResultSet.getBinaryStream(int)"})
  public void testGetBinaryStreamWithColumnIndex() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(SQLFeatureNotSupportedException.class, () -> libSqlResultSet.getBinaryStream(1));
  }

  /**
   * Test {@link LibSqlResultSet#getBinaryStream(String)} with {@code columnLabel}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getBinaryStream(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.io.InputStream LibSqlResultSet.getBinaryStream(String)"})
  public void testGetBinaryStreamWithColumnLabel() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> libSqlResultSet.getBinaryStream("Column Label"));
  }

  /**
   * Test {@link LibSqlResultSet#getMetaData()}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getMetaData()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"LibSqlResultSetMetaData LibSqlResultSet.getMetaData()"})
  public void testGetMetaData() throws SQLException {
    // Arrange, Act and Assert
    assertEquals(0, libSqlResultSet.getMetaData().getColumnCount());
  }

  /**
   * Test {@link LibSqlResultSet#getObject(int)} with {@code columnIndex}.
   *
   * <p>Method under test: {@link LibSqlResultSet#getObject(int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"Object LibSqlResultSet.getObject(int)"})
  public void testGetObjectWithColumnIndex() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getObject(1));
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getObject(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then calls {@link LibSqlExecutionResult#getRows()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getObject(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"Object LibSqlResultSet.getObject(String)"})
  public void testGetObjectWithColumnLabel_givenArrayListAdd42_when42_thenCallsGetRows()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getRows()).thenReturn(new ArrayList<>());
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getObject("42"));
    verify(libSqlExecutionResult).getColumns();
    verify(libSqlExecutionResult).getRows();
  }

  /**
   * Test {@link LibSqlResultSet#getObject(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getObject(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"Object LibSqlResultSet.getObject(String)"})
  public void testGetObjectWithColumnLabel_givenArrayListAddFoo_whenColumnLabel()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getObject("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#getObject(String)} with {@code columnLabel}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#getObject(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"Object LibSqlResultSet.getObject(String)"})
  public void testGetObjectWithColumnLabel_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.getObject("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#findColumn(String)}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code 42}.
   *   <li>When {@code 42}.
   *   <li>Then return one.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#findColumn(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSet.findColumn(String)"})
  public void testFindColumn_givenArrayListAdd42_when42_thenReturnOne() throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("42");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act
    int actualFindColumnResult = libSqlResultSet.findColumn("42");

    // Assert
    verify(libSqlExecutionResult).getColumns();
    assertEquals(1, actualFindColumnResult);
  }

  /**
   * Test {@link LibSqlResultSet#findColumn(String)}.
   *
   * <ul>
   *   <li>Given {@link ArrayList#ArrayList()} add {@code foo}.
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#findColumn(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSet.findColumn(String)"})
  public void testFindColumn_givenArrayListAddFoo_whenColumnLabel_thenThrowLibSqlException()
      throws SQLException {
    // Arrange
    ArrayList<String> stringList = new ArrayList<>();
    stringList.add("foo");
    when(libSqlExecutionResult.getColumns()).thenReturn(stringList);

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.findColumn("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }

  /**
   * Test {@link LibSqlResultSet#findColumn(String)}.
   *
   * <ul>
   *   <li>When {@code Column Label}.
   *   <li>Then throw {@link LibSqlException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlResultSet#findColumn(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"int LibSqlResultSet.findColumn(String)"})
  public void testFindColumn_whenColumnLabel_thenThrowLibSqlException() throws SQLException {
    // Arrange
    when(libSqlExecutionResult.getColumns()).thenReturn(new ArrayList<>());

    // Act and Assert
    assertThrows(LibSqlException.class, () -> libSqlResultSet.findColumn("Column Label"));
    verify(libSqlExecutionResult).getColumns();
  }
}
