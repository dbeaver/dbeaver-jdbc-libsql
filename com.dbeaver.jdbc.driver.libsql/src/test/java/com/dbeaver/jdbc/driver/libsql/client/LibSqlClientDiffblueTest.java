package com.dbeaver.jdbc.driver.libsql.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class LibSqlClientDiffblueTest {
  /**
   * Test {@link LibSqlClient#LibSqlClient(URL, String)}.
   *
   * <p>Method under test: {@link LibSqlClient#LibSqlClient(URL, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlClient.<init>(URL, String)"})
  public void testNewLibSqlClient() throws MalformedURLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();

    // Act
    LibSqlClient actualLibSqlClient = new LibSqlClient(url, "ABC123");

    // Assert
    assertEquals("ABC123", actualLibSqlClient.getAuthToken());
    assertEquals("DBeaver LibSQL JDBC driver 1.1.2", actualLibSqlClient.getUserAgent());
    assertSame(url, actualLibSqlClient.getUrl());
  }

  /**
   * Test getters and setters.
   *
   * <p>Methods under test:
   *
   * <ul>
   *   <li>{@link LibSqlClient#setUserAgent(String)}
   *   <li>{@link LibSqlClient#getAuthToken()}
   *   <li>{@link LibSqlClient#getClient()}
   *   <li>{@link LibSqlClient#getClientExecutor()}
   *   <li>{@link LibSqlClient#getUrl()}
   *   <li>{@link LibSqlClient#getUserAgent()}
   * </ul>
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "String LibSqlClient.getAuthToken()",
    "java.net.http.HttpClient LibSqlClient.getClient()",
    "java.util.concurrent.ExecutorService LibSqlClient.getClientExecutor()",
    "URL LibSqlClient.getUrl()",
    "String LibSqlClient.getUserAgent()",
    "void LibSqlClient.setUserAgent(String)"
  })
  public void testGettersAndSetters() throws MalformedURLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    // Act
    libSqlClient.setUserAgent("User Agent");
    String actualAuthToken = libSqlClient.getAuthToken();
    libSqlClient.getClient();
    libSqlClient.getClientExecutor();
    URL actualUrl = libSqlClient.getUrl();

    // Assert
    assertEquals("ABC123", actualAuthToken);
    assertEquals("User Agent", libSqlClient.getUserAgent());
    String expectedToStringResult =
        String.join(
            "",
            "file:",
            Paths.get(System.getProperty("java.io.tmpdir"), "test.txt")
                .toString()
                .concat(File.separator));
    assertEquals(expectedToStringResult, actualUrl.toString());
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given {@code 42}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@code 42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_given42_whenHashMap42Is42() throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put("42", "42");

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given {@code A}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@code A}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_givenA_whenHashMap42IsA() throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put("42", (byte) 'A');

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given empty string.
   *   <li>When {@link HashMap#HashMap()} empty string is {@code 42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_givenEmptyString_whenHashMapEmptyStringIs42()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put("", "42");

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given {@code false}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@code false}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_givenFalse_whenHashMap42IsFalse()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put("42", false);

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given {@link Double#NaN}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@link Double#NaN}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_givenNaN_whenHashMap42IsNaN() throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put("42", Double.NaN);

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given {@code null}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@code null}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_givenNull_whenHashMap42IsNull()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put("42", null);

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given one.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is one.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_givenOne_whenHashMap42IsOne() throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put("42", 1);

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given one.
   *   <li>When {@link HashMap#HashMap()} one is {@code 42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_givenOne_whenHashMapOneIs42() throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put(1, "42");

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given {@link SimpleEntry#SimpleEntry(Object, Object)} with {@code 42} and {@code 42}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@link SimpleEntry#SimpleEntry(Object,
   *       Object)} with {@code 42} and {@code 42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_givenSimpleEntryWith42And42_whenHashMap42IsSimpleEntryWith42And42()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put("42", new SimpleEntry<>("42", "42"));

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given ten.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is ten.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_givenTen_whenHashMap42IsTen() throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put("42", 10.0d);

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>Given {@code true}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@code true}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_givenTrue_whenHashMap42IsTrue()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> parameters = new HashMap<>();
    parameters.put("42", true);

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", parameters));
  }

  /**
   * Test {@link LibSqlClient#execute(String, Map)}.
   *
   * <ul>
   *   <li>When {@link HashMap#HashMap()}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#execute(String, Map)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult LibSqlClient.execute(String, Map)"
  })
  public void testExecute_whenHashMap() throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    // Act and Assert
    assertThrows(SQLException.class, () -> libSqlClient.execute("Stmt", new HashMap<>()));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch() throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> new LibSqlClient(url, "").executeBatch(new String[] {"Stmts"}, null));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given {@code 42}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@code 42}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_given42_whenHashMap42Is42_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put("42", "42");

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given {@code A}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@code A}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_givenA_whenHashMap42IsA_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put("42", (byte) 'A');

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given empty string.
   *   <li>When {@link HashMap#HashMap()} empty string is {@code 42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_givenEmptyString_whenHashMapEmptyStringIs42()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put("", "42");

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given {@code false}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@code false}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_givenFalse_whenHashMap42IsFalse_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put("42", false);

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given {@link Double#NaN}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@link Double#NaN}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_givenNaN_whenHashMap42IsNaN_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put("42", Double.NaN);

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given {@code null}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@code null}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_givenNull_whenHashMap42IsNull_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put("42", null);

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given one.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is one.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_givenOne_whenHashMap42IsOne_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put("42", 1);

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given one.
   *   <li>When {@link HashMap#HashMap()} one is {@code 42}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_givenOne_whenHashMapOneIs42_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put(1, "42");

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given {@link SimpleEntry#SimpleEntry(Object, Object)} with {@code 42} and {@code 42}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_givenSimpleEntryWith42And42()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put("42", new SimpleEntry<>("42", "42"));

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given ten.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is ten.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_givenTen_whenHashMap42IsTen_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put("42", 10.0d);

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>Given {@code true}.
   *   <li>When {@link HashMap#HashMap()} {@code 42} is {@code true}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_givenTrue_whenHashMap42IsTrue_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    HashMap<Object, Object> objectObjectMap = new HashMap<>();
    objectObjectMap.put("42", true);

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {objectObjectMap}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>When array of {@link String} with {@code statements} and {@code Stmts}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_whenArrayOfStringWithStatementsAndStmts_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    // Act and Assert
    assertThrows(
        SQLException.class,
        () ->
            libSqlClient.executeBatch(
                new String[] {"statements", "Stmts"}, new Map[] {new HashMap<>()}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>When array of {@link String} with {@code Stmts}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_whenArrayOfStringWithStmts_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {"Stmts"}, new Map[] {new HashMap<>()}));
  }

  /**
   * Test {@link LibSqlClient#executeBatch(String[], Map[])}.
   *
   * <ul>
   *   <li>When empty array of {@link String}.
   *   <li>Then throw {@link SQLException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlClient#executeBatch(String[], Map[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "com.dbeaver.jdbc.driver.libsql.client.LibSqlExecutionResult[] LibSqlClient.executeBatch(String[], Map[])"
  })
  public void testExecuteBatch_whenEmptyArrayOfString_thenThrowSQLException()
      throws MalformedURLException, SQLException {
    // Arrange
    URL url = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();
    LibSqlClient libSqlClient = new LibSqlClient(url, "ABC123");

    // Act and Assert
    assertThrows(
        SQLException.class,
        () -> libSqlClient.executeBatch(new String[] {}, new Map[] {new HashMap<>()}));
  }
}
