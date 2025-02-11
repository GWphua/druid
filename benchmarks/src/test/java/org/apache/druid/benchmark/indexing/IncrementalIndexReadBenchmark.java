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

package org.apache.druid.benchmark.indexing;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SearchQueryDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCreator;
import org.apache.druid.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class IncrementalIndexReadBenchmark
{
  @Param({"750000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  @Param({"true", "false"})
  private boolean rollup;

  @Param({"onheap", "offheap"})
  private String indexType;

  private static final Logger log = new Logger(IncrementalIndexReadBenchmark.class);
  private static final int RNG_SEED = 9999;

  private AppendableIndexSpec appendableIndexSpec;
  private IncrementalIndex incIndex;
  private GeneratorSchemaInfo schemaInfo;

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + +System.currentTimeMillis());

    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schema);

    // Creates an AppendableIndexSpec that corresponds to the indexType parametrization.
    // It is used in {@code makeIncIndex()} to instanciate an incremental-index of the specified type.
    appendableIndexSpec = IncrementalIndexCreator.parseIndexType(indexType);

    DataGenerator gen = new DataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval(),
        rowsPerSegment
    );

    incIndex = makeIncIndex();
    gen.addToIndex(incIndex, rowsPerSegment);
  }

  @TearDown
  public void tearDown()
  {
    if (incIndex != null) {
      incIndex.close();
    }
  }

  private IncrementalIndex makeIncIndex()
  {
    return appendableIndexSpec.builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(schemaInfo.getAggsArray())
                .withRollup(rollup)
                .build()
        )
        .setMaxRowCount(rowsPerSegment)
        .build();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void read(Blackhole blackhole)
  {
    final CursorFactory cursorFactory = new IncrementalIndexCursorFactory(incIndex);
    try (final CursorHolder cursorHolder = makeCursor(cursorFactory, null)) {
      Cursor cursor = cursorHolder.asCursor();

      List<DimensionSelector> selectors = new ArrayList<>();
      selectors.add(makeDimensionSelector(cursor, "dimSequential"));
      selectors.add(makeDimensionSelector(cursor, "dimZipf"));
      selectors.add(makeDimensionSelector(cursor, "dimUniform"));
      selectors.add(makeDimensionSelector(cursor, "dimSequentialHalfNull"));

      cursor.reset();
      while (!cursor.isDone()) {
        for (DimensionSelector selector : selectors) {
          IndexedInts row = selector.getRow();
          blackhole.consume(selector.lookupName(row.get(0)));
        }
        cursor.advance();
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void readWithFilters(Blackhole blackhole)
  {
    DimFilter filter = new OrDimFilter(
        Arrays.asList(
            new BoundDimFilter("dimSequential", "-1", "-1", true, true, null, null, StringComparators.ALPHANUMERIC),
            new JavaScriptDimFilter("dimSequential", "function(x) { return false }", null, JavaScriptConfig.getEnabledInstance()),
            new RegexDimFilter("dimSequential", "X", null),
            new SearchQueryDimFilter("dimSequential", new ContainsSearchQuerySpec("X", false), null),
            new InDimFilter("dimSequential", Collections.singletonList("X"), null)
        )
    );

    IncrementalIndexCursorFactory cursorFactory = new IncrementalIndexCursorFactory(incIndex);
    try (final CursorHolder cursorHolder = makeCursor(cursorFactory, filter)) {
      Cursor cursor = cursorHolder.asCursor();

      List<DimensionSelector> selectors = new ArrayList<>();
      selectors.add(makeDimensionSelector(cursor, "dimSequential"));
      selectors.add(makeDimensionSelector(cursor, "dimZipf"));
      selectors.add(makeDimensionSelector(cursor, "dimUniform"));
      selectors.add(makeDimensionSelector(cursor, "dimSequentialHalfNull"));

      cursor.reset();
      while (!cursor.isDone()) {
        for (DimensionSelector selector : selectors) {
          IndexedInts row = selector.getRow();
          blackhole.consume(selector.lookupName(row.get(0)));
        }
        cursor.advance();
      }
    }
  }

  private CursorHolder makeCursor(CursorFactory factory, DimFilter filter)
  {
    CursorBuildSpec.CursorBuildSpecBuilder builder = CursorBuildSpec.builder()
                                                                    .setInterval(schemaInfo.getDataInterval());
    if (filter != null) {
      builder.setFilter(filter.toFilter());
    }
    return factory.makeCursorHolder(builder.build());
  }

  private static DimensionSelector makeDimensionSelector(Cursor cursor, String name)
  {
    return cursor.getColumnSelectorFactory().makeDimensionSelector(new DefaultDimensionSpec(name, null));
  }
}
