package com.dbeaver.jdbc.driver.libsql.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

public class LibSqlStreamInputDiffblueTest {
  /**
   * Test getters and setters.
   *
   * <p>Methods under test:
   *
   * <ul>
   *   <li>{@link LibSqlStreamInput#LibSqlStreamInput(InputStream, long)}
   *   <li>{@link LibSqlStreamInput#getLength()}
   *   <li>{@link LibSqlStreamInput#getStream()}
   * </ul>
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlStreamInput.<init>(InputStream, long)",
    "long LibSqlStreamInput.getLength()",
    "InputStream LibSqlStreamInput.getStream()"
  })
  public void testGettersAndSetters() throws IOException {
    // Arrange
    ByteArrayInputStream stream = new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8"));

    // Act
    LibSqlStreamInput actualLibSqlStreamInput = new LibSqlStreamInput(stream, 3L);
    long actualLength = actualLibSqlStreamInput.getLength();
    InputStream actualStream = actualLibSqlStreamInput.getStream();

    // Assert
    assertEquals(3L, actualLength);
    assertEquals(8, actualStream.read(new byte[8]));
    assertSame(stream, actualStream);
  }

  /**
   * Test {@link LibSqlStreamInput#toString()}.
   *
   * <ul>
   *   <li>Given {@link ByteArrayInputStream#ByteArrayInputStream(byte[])} with {@code AXAXAXAX}
   *       Bytes is {@code UTF-8}.
   *   <li>Then return {@code AXAXAXAX}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStreamInput#toString()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlStreamInput.toString()"})
  public void testToString_givenByteArrayInputStreamWithAxaxaxaxBytesIsUtf8_thenReturnAxaxaxax()
      throws IOException {
    // Arrange
    LibSqlStreamInput libSqlStreamInput =
        new LibSqlStreamInput(new ByteArrayInputStream("AXAXAXAX".getBytes("UTF-8")), 3L);

    // Act and Assert
    assertEquals("AXAXAXAX", libSqlStreamInput.toString());
    int actualReadResult = libSqlStreamInput.getStream().read(new byte[] {});
    assertEquals(-1, actualReadResult);
  }

  /**
   * Test {@link LibSqlStreamInput#toString()}.
   *
   * <ul>
   *   <li>Given {@link DataInputStream} {@link DataInputStream#read(byte[])} throw {@link
   *       IOException#IOException()}.
   *   <li>Then return {@code null}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlStreamInput#toString()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"String LibSqlStreamInput.toString()"})
  public void testToString_givenDataInputStreamReadThrowIOException_thenReturnNull()
      throws IOException {
    // Arrange
    DataInputStream stream = mock(DataInputStream.class);
    when(stream.read(Mockito.<byte[]>any())).thenThrow(new IOException());

    // Act
    String actualToStringResult = new LibSqlStreamInput(stream, 3L).toString();

    // Assert
    verify(stream).read(isA(byte[].class));
    assertNull(actualToStringResult);
  }
}
