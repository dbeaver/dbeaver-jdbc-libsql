package com.dbeaver.jdbc.driver.libsql;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class LibSqlConnectionDiffblueTest {
  /**
   * Test {@link LibSqlConnection#LibSqlConnection(LibSqlDriver, String, Map)}.
   *
   * <ul>
   *   <li>When {@code password}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlConnection#LibSqlConnection(LibSqlDriver, String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlConnection.<init>(LibSqlDriver, String, Map)"})
  public void testNewLibSqlConnection_whenPassword_thenThrowSQLException() throws SQLException {
    // Arrange
    LibSqlDriver driver = mock(LibSqlDriver.class);

    // Act and Assert
    assertThrows(
        SQLException.class, () -> new LibSqlConnection(driver, "password", new HashMap<>()));
  }
}
