package com.dbeaver.jdbc.driver.libsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class LibSqlDriverDiffblueTest {
  /**
   * Test {@link LibSqlDriver#acceptsURL(String)}.
   *
   * <ul>
   *   <li>When {@code https://example.org/example}.
   *   <li>Then return {@code false}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDriver#acceptsURL(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlDriver.acceptsURL(String)"})
  public void testAcceptsURL_whenHttpsExampleOrgExample_thenReturnFalse() {
    // Arrange, Act and Assert
    assertFalse(new LibSqlDriver().acceptsURL("https://example.org/example"));
  }

  /**
   * Test {@link LibSqlDriver#acceptsURL(String)}.
   *
   * <ul>
   *   <li>When {@code jdbc:dbeaver:libsql:U}.
   *   <li>Then return {@code true}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlDriver#acceptsURL(String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlDriver.acceptsURL(String)"})
  public void testAcceptsURL_whenJdbcDbeaverLibsqlU_thenReturnTrue() {
    // Arrange, Act and Assert
    assertTrue(new LibSqlDriver().acceptsURL("jdbc:dbeaver:libsql:U"));
  }

  /**
   * Test {@link LibSqlDriver#getPropertyInfo(String, Properties)}.
   *
   * <p>Method under test: {@link LibSqlDriver#getPropertyInfo(String, Properties)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "java.sql.DriverPropertyInfo[] LibSqlDriver.getPropertyInfo(String, Properties)"
  })
  public void testGetPropertyInfo() {
    // Arrange
    LibSqlDriver libSqlDriver = new LibSqlDriver();

    // Act and Assert
    assertEquals(
        0, libSqlDriver.getPropertyInfo("https://example.org/example", new Properties()).length);
  }

  /**
   * Test {@link LibSqlDriver#jdbcCompliant()}.
   *
   * <p>Method under test: {@link LibSqlDriver#jdbcCompliant()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"boolean LibSqlDriver.jdbcCompliant()"})
  public void testJdbcCompliant() {
    // Arrange, Act and Assert
    assertTrue(new LibSqlDriver().jdbcCompliant());
  }

  /**
   * Test {@link LibSqlDriver#getFullVersion()}.
   *
   * <p>Method under test: {@link LibSqlDriver#getFullVersion()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlDriver.getFullVersion()"})
  public void testGetFullVersion() {
    // Arrange, Act and Assert
    assertEquals("1.0.2 (DBeaver LibSQL JDBC driver)", new LibSqlDriver().getFullVersion());
  }

  /**
   * Test getters and setters.
   *
   * <p>Methods under test:
   *
   * <ul>
   *   <li>default or parameterless constructor of {@link LibSqlDriver}
   *   <li>{@link LibSqlDriver#getDriverName()}
   *   <li>{@link LibSqlDriver#getMajorVersion()}
   *   <li>{@link LibSqlDriver#getMicroVersion()}
   *   <li>{@link LibSqlDriver#getMinorVersion()}
   *   <li>{@link LibSqlDriver#getParentLogger()}
   * </ul>
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlDriver.<init>()",
    "String LibSqlDriver.getDriverName()",
    "int LibSqlDriver.getMajorVersion()",
    "int LibSqlDriver.getMicroVersion()",
    "int LibSqlDriver.getMinorVersion()",
    "Logger LibSqlDriver.getParentLogger()"
  })
  public void testGettersAndSetters() {
    // Arrange and Act
    LibSqlDriver actualLibSqlDriver = new LibSqlDriver();
    String actualDriverName = actualLibSqlDriver.getDriverName();
    int actualMajorVersion = actualLibSqlDriver.getMajorVersion();
    int actualMicroVersion = actualLibSqlDriver.getMicroVersion();
    int actualMinorVersion = actualLibSqlDriver.getMinorVersion();
    Logger actualParentLogger = actualLibSqlDriver.getParentLogger();

    // Assert
    Logger parent = actualParentLogger.getParent();
    assertEquals("", parent.getName());
    Level level = parent.getLevel();
    assertEquals("INFO", level.getLocalizedName());
    assertEquals("INFO", level.getName());
    assertEquals("INFO", level.toString());
    assertEquals("com.dbeaver.jdbc.upd.driver.driver", actualParentLogger.getName());
    assertEquals("sun.util.logging.resources.logging", level.getResourceBundleName());
    assertNull(actualParentLogger.getResourceBundleName());
    assertNull(parent.getResourceBundleName());
    assertNull(actualParentLogger.getResourceBundle());
    assertNull(parent.getResourceBundle());
    assertNull(actualParentLogger.getFilter());
    assertNull(parent.getFilter());
    assertNull(actualParentLogger.getLevel());
    assertNull(parent.getParent());
    assertEquals(0, actualMinorVersion);
    assertEquals(0, actualParentLogger.getHandlers().length);
    assertEquals(1, actualMajorVersion);
    assertEquals(1, parent.getHandlers().length);
    assertEquals(2, actualMicroVersion);
    assertTrue(actualParentLogger.getUseParentHandlers());
    assertTrue(parent.getUseParentHandlers());
    assertEquals(LibSqlConstants.DRIVER_NAME, actualDriverName);
  }
}
