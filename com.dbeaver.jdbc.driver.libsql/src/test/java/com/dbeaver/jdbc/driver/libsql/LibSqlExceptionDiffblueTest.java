package com.dbeaver.jdbc.driver.libsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class LibSqlExceptionDiffblueTest {
  /**
   * Test {@link LibSqlException#LibSqlException()}.
   *
   * <ul>
   *   <li>Then return Message is {@code null}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlException#LibSqlException()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlException.<init>()",
    "void LibSqlException.<init>(String)",
    "void LibSqlException.<init>(String, String)",
    "void LibSqlException.<init>(String, String, int)",
    "void LibSqlException.<init>(String, String, int, Throwable)",
    "void LibSqlException.<init>(String, String, Throwable)",
    "void LibSqlException.<init>(String, Throwable)",
    "void LibSqlException.<init>(Throwable)"
  })
  public void testNewLibSqlException_thenReturnMessageIsNull() {
    // Arrange and Act
    LibSqlException actualLibSqlException = new LibSqlException();

    // Assert
    assertNull(actualLibSqlException.getMessage());
    assertNull(actualLibSqlException.getSQLState());
    assertNull(actualLibSqlException.getCause());
    assertNull(actualLibSqlException.getNextException());
    assertEquals(0, actualLibSqlException.getErrorCode());
    assertEquals(0, actualLibSqlException.getSuppressed().length);
  }

  /**
   * Test {@link LibSqlException#LibSqlException(String)}.
   *
   * <ul>
   *   <li>When {@code Just cause}.
   *   <li>Then return SQLState is {@code null}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlException#LibSqlException(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlException.<init>()",
    "void LibSqlException.<init>(String)",
    "void LibSqlException.<init>(String, String)",
    "void LibSqlException.<init>(String, String, int)",
    "void LibSqlException.<init>(String, String, int, Throwable)",
    "void LibSqlException.<init>(String, String, Throwable)",
    "void LibSqlException.<init>(String, Throwable)",
    "void LibSqlException.<init>(Throwable)"
  })
  public void testNewLibSqlException_whenJustCause_thenReturnSQLStateIsNull() {
    // Arrange and Act
    LibSqlException actualLibSqlException = new LibSqlException("Just cause");

    // Assert
    assertEquals("Just cause", actualLibSqlException.getMessage());
    assertNull(actualLibSqlException.getSQLState());
    assertNull(actualLibSqlException.getCause());
    assertNull(actualLibSqlException.getNextException());
    assertEquals(0, actualLibSqlException.getErrorCode());
    assertEquals(0, actualLibSqlException.getSuppressed().length);
  }

  /**
   * Test {@link LibSqlException#LibSqlException(String, String)}.
   *
   * <ul>
   *   <li>When {@code SQLState}.
   *   <li>Then return {@code SQLState}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlException#LibSqlException(String, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlException.<init>()",
    "void LibSqlException.<init>(String)",
    "void LibSqlException.<init>(String, String)",
    "void LibSqlException.<init>(String, String, int)",
    "void LibSqlException.<init>(String, String, int, Throwable)",
    "void LibSqlException.<init>(String, String, Throwable)",
    "void LibSqlException.<init>(String, Throwable)",
    "void LibSqlException.<init>(Throwable)"
  })
  public void testNewLibSqlException_whenSQLState_thenReturnSQLState() {
    // Arrange and Act
    LibSqlException actualLibSqlException = new LibSqlException("Just cause", "SQLState");

    // Assert
    assertEquals("Just cause", actualLibSqlException.getMessage());
    assertEquals("SQLState", actualLibSqlException.getSQLState());
    assertNull(actualLibSqlException.getCause());
    assertNull(actualLibSqlException.getNextException());
    assertEquals(0, actualLibSqlException.getErrorCode());
    assertEquals(0, actualLibSqlException.getSuppressed().length);
  }

  /**
   * Test {@link LibSqlException#LibSqlException(String, String, int)}.
   *
   * <ul>
   *   <li>When {@code SQLState}.
   *   <li>Then return {@code SQLState}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlException#LibSqlException(String, String, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlException.<init>()",
    "void LibSqlException.<init>(String)",
    "void LibSqlException.<init>(String, String)",
    "void LibSqlException.<init>(String, String, int)",
    "void LibSqlException.<init>(String, String, int, Throwable)",
    "void LibSqlException.<init>(String, String, Throwable)",
    "void LibSqlException.<init>(String, Throwable)",
    "void LibSqlException.<init>(Throwable)"
  })
  public void testNewLibSqlException_whenSQLState_thenReturnSQLState2() {
    // Arrange and Act
    LibSqlException actualLibSqlException = new LibSqlException("Just cause", "SQLState", 3);

    // Assert
    assertEquals("Just cause", actualLibSqlException.getMessage());
    assertEquals("SQLState", actualLibSqlException.getSQLState());
    assertNull(actualLibSqlException.getCause());
    assertNull(actualLibSqlException.getNextException());
    assertEquals(0, actualLibSqlException.getSuppressed().length);
    assertEquals(3, actualLibSqlException.getErrorCode());
  }

  /**
   * Test {@link LibSqlException#LibSqlException(String, String, int, Throwable)}.
   *
   * <ul>
   *   <li>When {@code Sql State}.
   *   <li>Then return {@code Sql State}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlException#LibSqlException(String, String, int, Throwable)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlException.<init>()",
    "void LibSqlException.<init>(String)",
    "void LibSqlException.<init>(String, String)",
    "void LibSqlException.<init>(String, String, int)",
    "void LibSqlException.<init>(String, String, int, Throwable)",
    "void LibSqlException.<init>(String, String, Throwable)",
    "void LibSqlException.<init>(String, Throwable)",
    "void LibSqlException.<init>(Throwable)"
  })
  public void testNewLibSqlException_whenSqlState_thenReturnSqlState() {
    // Arrange
    Throwable cause = new Throwable();

    // Act
    LibSqlException actualLibSqlException =
        new LibSqlException("Just cause", "Sql State", 3, cause);

    // Assert
    assertEquals("Just cause", actualLibSqlException.getMessage());
    assertEquals("Sql State", actualLibSqlException.getSQLState());
    assertNull(actualLibSqlException.getNextException());
    assertEquals(0, actualLibSqlException.getSuppressed().length);
    assertEquals(3, actualLibSqlException.getErrorCode());
    assertSame(cause, actualLibSqlException.getCause());
  }

  /**
   * Test {@link LibSqlException#LibSqlException(String, String, Throwable)}.
   *
   * <ul>
   *   <li>When {@code Sql State}.
   *   <li>Then return {@code Sql State}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlException#LibSqlException(String, String, Throwable)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlException.<init>()",
    "void LibSqlException.<init>(String)",
    "void LibSqlException.<init>(String, String)",
    "void LibSqlException.<init>(String, String, int)",
    "void LibSqlException.<init>(String, String, int, Throwable)",
    "void LibSqlException.<init>(String, String, Throwable)",
    "void LibSqlException.<init>(String, Throwable)",
    "void LibSqlException.<init>(Throwable)"
  })
  public void testNewLibSqlException_whenSqlState_thenReturnSqlState2() {
    // Arrange
    Throwable cause = new Throwable();

    // Act
    LibSqlException actualLibSqlException = new LibSqlException("Just cause", "Sql State", cause);

    // Assert
    assertEquals("Just cause", actualLibSqlException.getMessage());
    assertEquals("Sql State", actualLibSqlException.getSQLState());
    assertNull(actualLibSqlException.getNextException());
    assertEquals(0, actualLibSqlException.getErrorCode());
    assertEquals(0, actualLibSqlException.getSuppressed().length);
    assertSame(cause, actualLibSqlException.getCause());
  }

  /**
   * Test {@link LibSqlException#LibSqlException(Throwable)}.
   *
   * <ul>
   *   <li>When {@link Throwable#Throwable()}.
   *   <li>Then return Message is {@code Throwable}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlException#LibSqlException(Throwable)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlException.<init>()",
    "void LibSqlException.<init>(String)",
    "void LibSqlException.<init>(String, String)",
    "void LibSqlException.<init>(String, String, int)",
    "void LibSqlException.<init>(String, String, int, Throwable)",
    "void LibSqlException.<init>(String, String, Throwable)",
    "void LibSqlException.<init>(String, Throwable)",
    "void LibSqlException.<init>(Throwable)"
  })
  public void testNewLibSqlException_whenThrowable_thenReturnMessageIsJavaLangThrowable() {
    // Arrange
    Throwable cause = new Throwable();

    // Act
    LibSqlException actualLibSqlException = new LibSqlException(cause);

    // Assert
    assertEquals("java.lang.Throwable", actualLibSqlException.getMessage());
    assertNull(actualLibSqlException.getSQLState());
    assertNull(actualLibSqlException.getNextException());
    assertEquals(0, actualLibSqlException.getErrorCode());
    assertEquals(0, actualLibSqlException.getSuppressed().length);
    assertSame(cause, actualLibSqlException.getCause());
  }

  /**
   * Test {@link LibSqlException#LibSqlException(String, Throwable)}.
   *
   * <ul>
   *   <li>When {@link Throwable#Throwable()}.
   *   <li>Then return SQLState is {@code null}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlException#LibSqlException(String, Throwable)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlException.<init>()",
    "void LibSqlException.<init>(String)",
    "void LibSqlException.<init>(String, String)",
    "void LibSqlException.<init>(String, String, int)",
    "void LibSqlException.<init>(String, String, int, Throwable)",
    "void LibSqlException.<init>(String, String, Throwable)",
    "void LibSqlException.<init>(String, Throwable)",
    "void LibSqlException.<init>(Throwable)"
  })
  public void testNewLibSqlException_whenThrowable_thenReturnSQLStateIsNull() {
    // Arrange
    Throwable cause = new Throwable();

    // Act
    LibSqlException actualLibSqlException = new LibSqlException("Just cause", cause);

    // Assert
    assertEquals("Just cause", actualLibSqlException.getMessage());
    assertNull(actualLibSqlException.getSQLState());
    assertNull(actualLibSqlException.getNextException());
    assertEquals(0, actualLibSqlException.getErrorCode());
    assertEquals(0, actualLibSqlException.getSuppressed().length);
    assertSame(cause, actualLibSqlException.getCause());
  }
}
