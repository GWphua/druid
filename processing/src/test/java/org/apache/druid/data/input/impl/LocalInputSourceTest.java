/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.utils.Streams;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LocalInputSourceTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSerdeAbsoluteBaseDir() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final LocalInputSource source = new LocalInputSource(new File("myFile").getAbsoluteFile(), "myFilter");
    final byte[] json = mapper.writeValueAsBytes(source);
    final LocalInputSource fromJson = (LocalInputSource) mapper.readValue(json, InputSource.class);
    Assert.assertEquals(source, fromJson);
  }

  @Test
  public void testSerdeRelativeBaseDir() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final LocalInputSource source = new LocalInputSource(new File("myFile"), "myFilter");
    final byte[] json = mapper.writeValueAsBytes(source);
    final LocalInputSource fromJson = (LocalInputSource) mapper.readValue(json, InputSource.class);
    Assert.assertEquals(source, fromJson);
    Assert.assertEquals(Collections.emptySet(), fromJson.getConfiguredSystemFields());
  }

  @Test
  public void testSerdeRelativeBaseDirWithSystemFields() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final LocalInputSource source = new LocalInputSource(
        new File("myFile"),
        "myFilter",
        null,
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.PATH))
    );
    final byte[] json = mapper.writeValueAsBytes(source);
    final LocalInputSource fromJson = (LocalInputSource) mapper.readValue(json, InputSource.class);
    Assert.assertEquals(source, fromJson);
    Assert.assertEquals(EnumSet.of(SystemField.URI, SystemField.PATH), fromJson.getConfiguredSystemFields());
  }

  @Test
  public void testSerdeMixedAbsoluteAndRelativeFiles() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final LocalInputSource source = new LocalInputSource(
        null,
        null,
        ImmutableList.of(
            new File("myFile1"),
            new File("myFile2").getAbsoluteFile()
        ),
        null
    );
    final byte[] json = mapper.writeValueAsBytes(source);
    final LocalInputSource fromJson = (LocalInputSource) mapper.readValue(json, InputSource.class);
    Assert.assertEquals(source, fromJson);
  }

  @Test
  public void testGetTypes()
  {
    final LocalInputSource source = new LocalInputSource(new File("myFile").getAbsoluteFile(), "myFilter");
    Assert.assertEquals(Collections.singleton(LocalInputSource.TYPE_KEY), source.getTypes());
  }

  @Test
  public void testSystemFields()
  {
    final LocalInputSource inputSource = new LocalInputSource(
        null,
        null,
        ImmutableList.of(
            new File("myFile1"),
            new File("myFile2").getAbsoluteFile()
        ),
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.PATH))
    );

    Assert.assertEquals(
        EnumSet.of(SystemField.URI, SystemField.PATH),
        inputSource.getConfiguredSystemFields()
    );

    final FileEntity entity = new FileEntity(new File("/tmp/foo"));

    Assert.assertEquals("file:/tmp/foo", inputSource.getSystemFieldValue(entity, SystemField.URI));
    Assert.assertEquals("/tmp/foo", inputSource.getSystemFieldValue(entity, SystemField.PATH));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(LocalInputSource.class).usingGetClass().withNonnullFields("files").verify();
  }

  @Test
  public void testCreateSplitsRespectingSplitHintSpec()
  {
    final long fileSize = 15;
    final HumanReadableBytes maxSplitSize = new HumanReadableBytes(50L);
    final List<File> files = mockFiles(10, fileSize);
    final LocalInputSource inputSource = new LocalInputSource(null, null, files, null);
    final List<InputSplit<List<File>>> splits = inputSource
        .createSplits(new NoopInputFormat(), new MaxSizeSplitHintSpec(maxSplitSize, null))
        .collect(Collectors.toList());
    Assert.assertEquals(4, splits.size());
    Assert.assertEquals(3, splits.get(0).get().size());
    Assert.assertEquals(3, splits.get(1).get().size());
    Assert.assertEquals(3, splits.get(2).get().size());
    Assert.assertEquals(1, splits.get(3).get().size());
  }

  @Test
  public void testEstimateNumSplitsRespectingSplitHintSpec()
  {
    final long fileSize = 13;
    final HumanReadableBytes maxSplitSize = new HumanReadableBytes(40L);
    final List<File> files = mockFiles(10, fileSize);
    final LocalInputSource inputSource = new LocalInputSource(null, null, files, null);
    Assert.assertEquals(
        4,
        inputSource.estimateNumSplits(new NoopInputFormat(), new MaxSizeSplitHintSpec(maxSplitSize, null))
    );
  }

  @Test
  public void testGetFileIteratorWithBothBaseDirAndDuplicateFilesIteratingFilesOnlyOnce() throws IOException
  {
    File baseDir = temporaryFolder.newFolder();
    List<File> filesInBaseDir = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final File file = File.createTempFile("local-input-source", ".data", baseDir);
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write("test");
      }
      filesInBaseDir.add(file);
    }
    List<File> files = filesInBaseDir.subList(0, 5);
    for (int i = 0; i < 3; i++) {
      final File file = File.createTempFile("local-input-source", ".data", baseDir);
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write("test");
      }
      files.add(file);
    }
    Set<File> expectedFiles = new HashSet<>(filesInBaseDir);
    expectedFiles.addAll(files);
    File.createTempFile("local-input-source", ".filtered", baseDir);
    Iterator<File> fileIterator = new LocalInputSource(baseDir, "*.data", files, null).getFileIterator();
    Set<File> actualFiles = Streams.sequentialStreamFrom(fileIterator).collect(Collectors.toSet());
    Assert.assertEquals(expectedFiles, actualFiles);
  }

  @Test
  public void testGetFileIteratorWithOnlyBaseDirIteratingAllFiles() throws IOException
  {
    File baseDir = temporaryFolder.newFolder();
    Set<File> filesInBaseDir = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      final File file = File.createTempFile("local-input-source", ".data", baseDir);
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write("test");
      }
      filesInBaseDir.add(file);
    }
    Iterator<File> fileIterator = new LocalInputSource(baseDir, "*", null, null).getFileIterator();
    Set<File> actualFiles = Streams.sequentialStreamFrom(fileIterator).collect(Collectors.toSet());
    Assert.assertEquals(filesInBaseDir, actualFiles);
  }

  @Test
  public void testGetFileIteratorWithOnlyFilesIteratingAllFiles() throws IOException
  {
    File baseDir = temporaryFolder.newFolder();
    List<File> filesInBaseDir = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final File file = File.createTempFile("local-input-source", ".data", baseDir);
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write("test");
      }
      filesInBaseDir.add(file);
    }
    Iterator<File> fileIterator = new LocalInputSource(null, null, filesInBaseDir, null).getFileIterator();
    List<File> actualFiles = Streams.sequentialStreamFrom(fileIterator).collect(Collectors.toList());
    Assert.assertEquals(filesInBaseDir, actualFiles);
  }

  @Test
  public void testFileIteratorWithEmptyFilesIteratingNonEmptyFilesOnly()
  {
    final List<File> files = mockFiles(10, 5);
    files.addAll(mockFiles(10, 0));
    final LocalInputSource inputSource = new LocalInputSource(null, null, files, null);
    List<File> iteratedFiles = Lists.newArrayList(inputSource.getFileIterator());
    Assert.assertTrue(iteratedFiles.stream().allMatch(file -> file.length() > 0));
  }

  @Test
  public void testRebuildSame()
  {
    final LocalInputSource src = new LocalInputSource(
        new File("myFile"),
        "myFilter",
        ImmutableList.of(new File("someFile")),
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.PATH))
    );
    LocalInputSource copy = new LocalInputSource(
        src.getBaseDir(), src.getFilter(), src.getFiles(), src.getSystemFields()
    );
    Assert.assertEquals(src, copy);
  }

  private static List<File> mockFiles(int numFiles, long fileSize)
  {
    final List<File> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      final File file = EasyMock.niceMock(File.class);
      EasyMock.expect(file.length()).andReturn(fileSize).anyTimes();
      EasyMock.replay(file);
      files.add(file);
    }
    return files;
  }
}
