package com.dbeaver.jdbc.driver.libsql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import com.dbeaver.jdbc.driver.libsql.client.LibSqlReaderInput;
import com.dbeaver.jdbc.driver.libsql.client.LibSqlStreamInput;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialRef;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class LibSqlPreparedStatementDiffblueTest {
  /**
   * Test {@link LibSqlPreparedStatement#addParameter(int, Object)} with {@code parameterIndex},
   * {@code value}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#addParameter(int, Object)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.addParameter(int, Object)"})
  public void testAddParameterWithParameterIndexValue() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.addParameter(1, "Value");

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals("Value", objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#addParameter(String, Object)} with {@code parameterName},
   * {@code value}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#addParameter(String, Object)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.addParameter(String, Object)"})
  public void testAddParameterWithParameterNameValue() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.addParameter("Parameter Name", "Value");

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals("Value", objectObjectMap.get("Parameter Name"));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setNull(int, int)} with {@code parameterIndex}, {@code
   * sqlType}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setNull(int, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setNull(int, int)"})
  public void testSetNullWithParameterIndexSqlType() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setNull(1, 1);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertNull(objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setNull(int, int, String)} with {@code parameterIndex},
   * {@code sqlType}, {@code typeName}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setNull(int, int, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setNull(int, int, String)"})
  public void testSetNullWithParameterIndexSqlTypeTypeName() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setNull(1, 1, "Type Name");

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertNull(objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setURL(int, URL)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setURL(int, URL)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setURL(int, URL)"})
  public void testSetURL() throws MalformedURLException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    URL x = Paths.get(System.getProperty("java.io.tmpdir"), "test.txt").toUri().toURL();

    // Act
    libSqlPreparedStatement.setURL(1, x);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertSame(x, objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setBoolean(int, boolean)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setBoolean(int, boolean)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setBoolean(int, boolean)"})
  public void testSetBoolean() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setBoolean(1, true);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertTrue((Boolean) objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setByte(int, byte)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setByte(int, byte)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setByte(int, byte)"})
  public void testSetByte() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setByte(1, (byte) 'A');

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals('A', ((Byte) objectObjectMap.get(1)).byteValue());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setShort(int, short)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setShort(int, short)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setShort(int, short)"})
  public void testSetShort() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setShort(1, (short) 1);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals((short) 1, ((Short) objectObjectMap.get(1)).shortValue());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setInt(int, int)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setInt(int, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setInt(int, int)"})
  public void testSetInt() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setInt(1, 2);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals(2, ((Integer) objectObjectMap.get(1)).intValue());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setLong(int, long)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setLong(int, long)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setLong(int, long)"})
  public void testSetLong() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setLong(1, 1L);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals(1L, ((Long) objectObjectMap.get(1)).longValue());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setFloat(int, float)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setFloat(int, float)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setFloat(int, float)"})
  public void testSetFloat() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setFloat(1, 10.0f);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals(10.0f, ((Float) objectObjectMap.get(1)).floatValue(), 0.0f);
  }

  /**
   * Test {@link LibSqlPreparedStatement#setDouble(int, double)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setDouble(int, double)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setDouble(int, double)"})
  public void testSetDouble() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setDouble(1, 2.0d);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals(2.0d, ((Double) objectObjectMap.get(1)).doubleValue(), 0.0);
  }

  /**
   * Test {@link LibSqlPreparedStatement#setBigDecimal(int, BigDecimal)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setBigDecimal(int, BigDecimal)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setBigDecimal(int, BigDecimal)"})
  public void testSetBigDecimal() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    BigDecimal x = new BigDecimal("2.3");

    // Act
    libSqlPreparedStatement.setBigDecimal(1, x);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertSame(x, objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setString(int, String)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setString(int, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setString(int, String)"})
  public void testSetString() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setString(1, "foo");

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals("foo", objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setBytes(int, byte[])}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setBytes(int, byte[])}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setBytes(int, byte[])"})
  public void testSetBytes() throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    byte[] x = "AXAXAXAX".getBytes("UTF-8");

    // Act
    libSqlPreparedStatement.setBytes(1, x);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertSame(x, objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setDate(int, Date)} with {@code parameterIndex}, {@code x}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setDate(int, Date)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setDate(int, Date)"})
  public void testSetDateWithParameterIndexX() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    Date x = new Date(1L);

    // Act
    libSqlPreparedStatement.setDate(1, x);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertSame(x, objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setDate(int, Date, Calendar)} with {@code parameterIndex},
   * {@code x}, {@code cal}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setDate(int, Date, Calendar)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setDate(int, Date, Calendar)"})
  public void testSetDateWithParameterIndexXCal() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    Date x = new Date(1L);

    // Act
    libSqlPreparedStatement.setDate(1, x, new GregorianCalendar(1, 1, 1));

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertSame(x, objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setTime(int, Time)} with {@code parameterIndex}, {@code x}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setTime(int, Time)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setTime(int, Time)"})
  public void testSetTimeWithParameterIndexX() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setTime(1, null);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertNull(objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setTimestamp(int, Timestamp)} with {@code parameterIndex},
   * {@code x}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setTimestamp(int, Timestamp)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setTimestamp(int, Timestamp)"})
  public void testSetTimestampWithParameterIndexX() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setTimestamp(1, null);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertNull(objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setAsciiStream(int, InputStream)} with {@code int}, {@code
   * InputStream}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setAsciiStream(int, InputStream)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setAsciiStream(int, InputStream)"})
  public void testSetAsciiStreamWithIntInputStream()
      throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    ByteArrayInputStream x = new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8"));

    // Act
    libSqlPreparedStatement.setAsciiStream(1, x);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlStreamInput);
    assertEquals(-1L, ((LibSqlStreamInput) getResult).getLength());
    assertSame(x, ((LibSqlStreamInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setAsciiStream(int, InputStream, int)} with {@code int},
   * {@code InputStream}, {@code int}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setAsciiStream(int, InputStream, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setAsciiStream(int, InputStream, int)"})
  public void testSetAsciiStreamWithIntInputStreamInt()
      throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    ByteArrayInputStream x = new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8"));

    // Act
    libSqlPreparedStatement.setAsciiStream(1, x, 3);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlStreamInput);
    assertEquals(3L, ((LibSqlStreamInput) getResult).getLength());
    assertSame(x, ((LibSqlStreamInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setAsciiStream(int, InputStream, long)} with {@code int},
   * {@code InputStream}, {@code long}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setAsciiStream(int, InputStream, long)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setAsciiStream(int, InputStream, long)"})
  public void testSetAsciiStreamWithIntInputStreamLong()
      throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    ByteArrayInputStream x = new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8"));

    // Act
    libSqlPreparedStatement.setAsciiStream(1, x, 3L);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlStreamInput);
    assertEquals(3L, ((LibSqlStreamInput) getResult).getLength());
    assertSame(x, ((LibSqlStreamInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setUnicodeStream(int, InputStream, int)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setUnicodeStream(int, InputStream, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setUnicodeStream(int, InputStream, int)"})
  public void testSetUnicodeStream() throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    ByteArrayInputStream x = new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8"));

    // Act
    libSqlPreparedStatement.setUnicodeStream(1, x, 3);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlStreamInput);
    assertEquals(3L, ((LibSqlStreamInput) getResult).getLength());
    assertSame(x, ((LibSqlStreamInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setBinaryStream(int, InputStream)} with {@code int}, {@code
   * InputStream}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setBinaryStream(int, InputStream)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setBinaryStream(int, InputStream)"})
  public void testSetBinaryStreamWithIntInputStream()
      throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    ByteArrayInputStream x = new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8"));

    // Act
    libSqlPreparedStatement.setBinaryStream(1, x);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlStreamInput);
    assertEquals(-1L, ((LibSqlStreamInput) getResult).getLength());
    assertSame(x, ((LibSqlStreamInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setBinaryStream(int, InputStream, int)} with {@code int},
   * {@code InputStream}, {@code int}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setBinaryStream(int, InputStream, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setBinaryStream(int, InputStream, int)"})
  public void testSetBinaryStreamWithIntInputStreamInt()
      throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    ByteArrayInputStream x = new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8"));

    // Act
    libSqlPreparedStatement.setBinaryStream(1, x, 3);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlStreamInput);
    assertEquals(3L, ((LibSqlStreamInput) getResult).getLength());
    assertSame(x, ((LibSqlStreamInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setBinaryStream(int, InputStream, long)} with {@code int},
   * {@code InputStream}, {@code long}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setBinaryStream(int, InputStream, long)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setBinaryStream(int, InputStream, long)"})
  public void testSetBinaryStreamWithIntInputStreamLong()
      throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    ByteArrayInputStream x = new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8"));

    // Act
    libSqlPreparedStatement.setBinaryStream(1, x, 3L);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlStreamInput);
    assertEquals(3L, ((LibSqlStreamInput) getResult).getLength());
    assertSame(x, ((LibSqlStreamInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setObject(int, Object)} with {@code int}, {@code Object}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setObject(int, Object)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setObject(int, Object)"})
  public void testSetObjectWithIntObject() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setObject(1, "42");

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals("42", objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setObject(int, Object, int)} with {@code int}, {@code
   * Object}, {@code int}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setObject(int, Object, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setObject(int, Object, int)"})
  public void testSetObjectWithIntObjectInt() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setObject(1, "42", 1);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals("42", objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setObject(int, Object, int, int)} with {@code int}, {@code
   * Object}, {@code int}, {@code int}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setObject(int, Object, int, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setObject(int, Object, int, int)"})
  public void testSetObjectWithIntObjectIntInt() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setObject(1, "42", 1, 3);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals("42", objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#addBatch()}.
   *
   * <ul>
   *   <li>Then throw {@link SQLFeatureNotSupportedException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#addBatch()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.addBatch()"})
  public void testAddBatch_thenThrowSQLFeatureNotSupportedException() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> new LibSqlPreparedStatement(null, "Sql").addBatch());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setCharacterStream(int, Reader)} with {@code int}, {@code
   * Reader}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setCharacterStream(int, Reader)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setCharacterStream(int, Reader)"})
  public void testSetCharacterStreamWithIntReader() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    StringReader reader = new StringReader("foo");

    // Act
    libSqlPreparedStatement.setCharacterStream(1, reader);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlReaderInput);
    assertEquals(-1L, ((LibSqlReaderInput) getResult).getLength());
    assertSame(reader, ((LibSqlReaderInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setCharacterStream(int, Reader, int)} with {@code int},
   * {@code Reader}, {@code int}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setCharacterStream(int, Reader, int)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setCharacterStream(int, Reader, int)"})
  public void testSetCharacterStreamWithIntReaderInt() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    StringReader reader = new StringReader("foo");

    // Act
    libSqlPreparedStatement.setCharacterStream(1, reader, 3);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlReaderInput);
    assertEquals(3L, ((LibSqlReaderInput) getResult).getLength());
    assertSame(reader, ((LibSqlReaderInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setCharacterStream(int, Reader, long)} with {@code int},
   * {@code Reader}, {@code long}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setCharacterStream(int, Reader, long)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setCharacterStream(int, Reader, long)"})
  public void testSetCharacterStreamWithIntReaderLong() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    StringReader reader = new StringReader("foo");

    // Act
    libSqlPreparedStatement.setCharacterStream(1, reader, 3L);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlReaderInput);
    assertEquals(3L, ((LibSqlReaderInput) getResult).getLength());
    assertSame(reader, ((LibSqlReaderInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setRef(int, Ref)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setRef(int, Ref)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setRef(int, Ref)"})
  public void testSetRef() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    SerialRef x = mock(SerialRef.class);

    // Act
    libSqlPreparedStatement.setRef(1, x);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertSame(x, objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setBlob(int, InputStream)} with {@code parameterIndex},
   * {@code inputStream}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setBlob(int, InputStream)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setBlob(int, InputStream)"})
  public void testSetBlobWithParameterIndexInputStream()
      throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    ByteArrayInputStream inputStream = new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8"));

    // Act
    libSqlPreparedStatement.setBlob(1, inputStream);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlStreamInput);
    assertEquals(-1L, ((LibSqlStreamInput) getResult).getLength());
    assertSame(inputStream, ((LibSqlStreamInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setBlob(int, InputStream, long)} with {@code
   * parameterIndex}, {@code inputStream}, {@code length}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setBlob(int, InputStream, long)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setBlob(int, InputStream, long)"})
  public void testSetBlobWithParameterIndexInputStreamLength()
      throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    ByteArrayInputStream inputStream = new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8"));

    // Act
    libSqlPreparedStatement.setBlob(1, inputStream, 3L);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlStreamInput);
    assertEquals(3L, ((LibSqlStreamInput) getResult).getLength());
    assertSame(inputStream, ((LibSqlStreamInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setBlob(int, Blob)} with {@code parameterIndex}, {@code x}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setBlob(int, Blob)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setBlob(int, Blob)"})
  public void testSetBlobWithParameterIndexX() throws UnsupportedEncodingException, SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    SerialBlob x = new SerialBlob("AXAXAXAX".getBytes("UTF-8"));

    // Act
    libSqlPreparedStatement.setBlob(1, x);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertSame(x, objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setClob(int, Reader)} with {@code parameterIndex}, {@code
   * reader}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setClob(int, Reader)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setClob(int, Reader)"})
  public void testSetClobWithParameterIndexReader() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    StringReader reader = new StringReader("foo");

    // Act
    libSqlPreparedStatement.setClob(1, reader);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlReaderInput);
    assertEquals(-1L, ((LibSqlReaderInput) getResult).getLength());
    assertSame(reader, ((LibSqlReaderInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setClob(int, Reader, long)} with {@code parameterIndex},
   * {@code reader}, {@code length}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setClob(int, Reader, long)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setClob(int, Reader, long)"})
  public void testSetClobWithParameterIndexReaderLength() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    StringReader reader = new StringReader("foo");

    // Act
    libSqlPreparedStatement.setClob(1, reader, 3L);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlReaderInput);
    assertEquals(3L, ((LibSqlReaderInput) getResult).getLength());
    assertSame(reader, ((LibSqlReaderInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setClob(int, Clob)} with {@code parameterIndex}, {@code x}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setClob(int, Clob)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setClob(int, Clob)"})
  public void testSetClobWithParameterIndexX() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    SerialClob x = new SerialClob("AZAZ".toCharArray());

    // Act
    libSqlPreparedStatement.setClob(1, x);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertSame(x, objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#getMetaData()}.
   *
   * <ul>
   *   <li>Then throw {@link SQLFeatureNotSupportedException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#getMetaData()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.ResultSetMetaData LibSqlPreparedStatement.getMetaData()"})
  public void testGetMetaData_thenThrowSQLFeatureNotSupportedException() throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> new LibSqlPreparedStatement(null, "Sql").getMetaData());
  }

  /**
   * Test {@link LibSqlPreparedStatement#getParameterMetaData()}.
   *
   * <ul>
   *   <li>Then throw {@link SQLFeatureNotSupportedException}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#getParameterMetaData()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.sql.ParameterMetaData LibSqlPreparedStatement.getParameterMetaData()"})
  public void testGetParameterMetaData_thenThrowSQLFeatureNotSupportedException()
      throws SQLException {
    // Arrange, Act and Assert
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> new LibSqlPreparedStatement(null, "Sql").getParameterMetaData());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setRowId(int, RowId)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setRowId(int, RowId)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setRowId(int, RowId)"})
  public void testSetRowId() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setRowId(1, null);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertNull(objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setNString(int, String)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setNString(int, String)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setNString(int, String)"})
  public void testSetNString() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setNString(1, "42");

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertEquals("42", objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setNCharacterStream(int, Reader)} with {@code
   * parameterIndex}, {@code value}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setNCharacterStream(int, Reader)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setNCharacterStream(int, Reader)"})
  public void testSetNCharacterStreamWithParameterIndexValue() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    StringReader value = new StringReader("foo");

    // Act
    libSqlPreparedStatement.setNCharacterStream(1, value);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlReaderInput);
    assertEquals(-1L, ((LibSqlReaderInput) getResult).getLength());
    assertSame(value, ((LibSqlReaderInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setNCharacterStream(int, Reader, long)} with {@code
   * parameterIndex}, {@code value}, {@code length}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setNCharacterStream(int, Reader, long)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setNCharacterStream(int, Reader, long)"})
  public void testSetNCharacterStreamWithParameterIndexValueLength() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    StringReader value = new StringReader("foo");

    // Act
    libSqlPreparedStatement.setNCharacterStream(1, value, 3L);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlReaderInput);
    assertEquals(3L, ((LibSqlReaderInput) getResult).getLength());
    assertSame(value, ((LibSqlReaderInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setNClob(int, Reader)} with {@code parameterIndex}, {@code
   * reader}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setNClob(int, Reader)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setNClob(int, Reader)"})
  public void testSetNClobWithParameterIndexReader() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    StringReader reader = new StringReader("foo");

    // Act
    libSqlPreparedStatement.setNClob(1, reader);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlReaderInput);
    assertEquals(-1L, ((LibSqlReaderInput) getResult).getLength());
    assertSame(reader, ((LibSqlReaderInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setNClob(int, Reader, long)} with {@code parameterIndex},
   * {@code reader}, {@code length}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setNClob(int, Reader, long)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setNClob(int, Reader, long)"})
  public void testSetNClobWithParameterIndexReaderLength() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    StringReader reader = new StringReader("foo");

    // Act
    libSqlPreparedStatement.setNClob(1, reader, 3L);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    Object getResult = objectObjectMap.get(1);
    assertTrue(getResult instanceof LibSqlReaderInput);
    assertEquals(3L, ((LibSqlReaderInput) getResult).getLength());
    assertSame(reader, ((LibSqlReaderInput) getResult).getStream());
  }

  /**
   * Test {@link LibSqlPreparedStatement#setNClob(int, NClob)} with {@code parameterIndex}, {@code
   * value}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setNClob(int, NClob)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setNClob(int, NClob)"})
  public void testSetNClobWithParameterIndexValue() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");
    NClob value = mock(NClob.class);

    // Act
    libSqlPreparedStatement.setNClob(1, value);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertSame(value, objectObjectMap.get(1));
  }

  /**
   * Test {@link LibSqlPreparedStatement#setSQLXML(int, SQLXML)}.
   *
   * <p>Method under test: {@link LibSqlPreparedStatement#setSQLXML(int, SQLXML)}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"void LibSqlPreparedStatement.setSQLXML(int, SQLXML)"})
  public void testSetSQLXML() throws SQLException {
    // Arrange
    LibSqlPreparedStatement libSqlPreparedStatement = new LibSqlPreparedStatement(null, "Sql");

    // Act
    libSqlPreparedStatement.setSQLXML(1, null);

    // Assert
    Map<Object, Object> objectObjectMap = libSqlPreparedStatement.parameters;
    assertEquals(1, objectObjectMap.size());
    assertNull(objectObjectMap.get(1));
  }
}
