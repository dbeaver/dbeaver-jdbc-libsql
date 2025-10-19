package com.dbeaver.jdbc.driver.libsql.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import com.diffblue.cover.annotations.ContributionFromDiffblue;
import com.diffblue.cover.annotations.ManagedByDiffblue;
import com.diffblue.cover.annotations.MethodsUnderTest;
import java.io.FileDescriptor;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class LibSqlReaderInputDiffblueTest {
  /**
   * Test getters and setters.
   *
   * <p>Methods under test:
   *
   * <ul>
   *   <li>{@link LibSqlReaderInput#LibSqlReaderInput(Reader, long)}
   *   <li>{@link LibSqlReaderInput#getLength()}
   *   <li>{@link LibSqlReaderInput#getStream()}
   * </ul>
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({
    "void LibSqlReaderInput.<init>(Reader, long)",
    "long LibSqlReaderInput.getLength()",
    "Reader LibSqlReaderInput.getStream()"
  })
  public void testGettersAndSetters() throws IOException {
    // Arrange
    StringReader stream = new StringReader("foo");

    // Act
    LibSqlReaderInput actualLibSqlReaderInput = new LibSqlReaderInput(stream, 3L);
    long actualLength = actualLibSqlReaderInput.getLength();
    Reader actualStream = actualLibSqlReaderInput.getStream();

    // Assert
    assertEquals(3L, actualLength);
    assertTrue(actualStream.ready());
    assertSame(stream, actualStream);
  }

  /**
   * Test {@link LibSqlReaderInput#toString()}.
   *
   * <ul>
   *   <li>Given {@link FileReader#FileReader(FileDescriptor)} with {@link
   *       FileDescriptor#FileDescriptor()}.
   *   <li>Then return {@code Stream Closed}.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlReaderInput#toString()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.lang.String LibSqlReaderInput.toString()"})
  public void testToString_givenFileReaderWithFileDescriptor_thenReturnStreamClosed() {
    // Arrange, Act and Assert
    assertEquals(
        "Stream Closed",
        new LibSqlReaderInput(new FileReader(new FileDescriptor()), 3L).toString());
  }

  /**
   * Test {@link LibSqlReaderInput#toString()}.
   *
   * <ul>
   *   <li>Given {@link LibSqlReaderInput#LibSqlReaderInput(Reader, long)} with stream is {@link
   *       StringReader#StringReader(String)} and length is three.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlReaderInput#toString()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.lang.String LibSqlReaderInput.toString()"})
  public void testToString_givenLibSqlReaderInputWithStreamIsStringReaderAndLengthIsThree() {
    // Arrange, Act and Assert
    assertEquals("foo", new LibSqlReaderInput(new StringReader("foo"), 3L).toString());
  }

  /**
   * Test {@link LibSqlReaderInput#toString()}.
   *
   * <ul>
   *   <li>Given {@link LibSqlReaderInput#LibSqlReaderInput(Reader, long)} with stream is {@link
   *       StringReader#StringReader(String)} and length is zero.
   * </ul>
   *
   * <p>Method under test: {@link LibSqlReaderInput#toString()}
   */
  @Test
  @Category(ContributionFromDiffblue.class)
  @ManagedByDiffblue
  @MethodsUnderTest({"java.lang.String LibSqlReaderInput.toString()"})
  public void testToString_givenLibSqlReaderInputWithStreamIsStringReaderAndLengthIsZero() {
    // Arrange, Act and Assert
    assertEquals("foo", new LibSqlReaderInput(new StringReader("foo"), 0L).toString());
  }
}
