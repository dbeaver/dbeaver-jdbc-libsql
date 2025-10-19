package com.dbeaver.jdbc.driver.libsql.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class LibSqlExecutionResultDiffblueTest {
  /**
   * Test {@link LibSqlExecutionResult#getRowsWritten()}.
   *
   * <p>Method under test: {@link LibSqlExecutionResult#getRowsWritten()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"double LibSqlExecutionResult.getRowsWritten()"})
  public void testGetRowsWritten() {
    // Arrange, Act and Assert
    assertEquals(0.0d, new LibSqlExecutionResult().getRowsWritten(), 0.0);
  }

  /**
   * Test getters and setters.
   *
   * <p>Methods under test:
   *
   * <ul>
   *   <li>default or parameterless constructor of {@link LibSqlExecutionResult}
   *   <li>{@link LibSqlExecutionResult#getColumns()}
   *   <li>{@link LibSqlExecutionResult#getQueryDurationMs()}
   *   <li>{@link LibSqlExecutionResult#getRows()}
   *   <li>{@link LibSqlExecutionResult#getRowsRead()}
   *   <li>{@link LibSqlExecutionResult#getUpdateCount()}
   * </ul>
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlExecutionResult.<init>()",
    "List LibSqlExecutionResult.getColumns()",
    "double LibSqlExecutionResult.getQueryDurationMs()",
    "List LibSqlExecutionResult.getRows()",
    "long LibSqlExecutionResult.getRowsRead()",
    "long LibSqlExecutionResult.getUpdateCount()"
  })
  public void testGettersAndSetters() {
    // Arrange and Act
    LibSqlExecutionResult actualLibSqlExecutionResult = new LibSqlExecutionResult();
    List<String> actualColumns = actualLibSqlExecutionResult.getColumns();
    double actualQueryDurationMs = actualLibSqlExecutionResult.getQueryDurationMs();
    List<Object[]> actualRows = actualLibSqlExecutionResult.getRows();
    long actualRowsRead = actualLibSqlExecutionResult.getRowsRead();

    // Assert
    assertNull(actualRows);
    assertNull(actualColumns);
    assertEquals(0.0d, actualQueryDurationMs, 0.0);
    assertEquals(0L, actualRowsRead);
    assertEquals(0L, actualLibSqlExecutionResult.getUpdateCount());
  }
}
