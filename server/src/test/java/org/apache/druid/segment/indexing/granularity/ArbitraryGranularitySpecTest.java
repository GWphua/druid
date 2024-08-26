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

package org.apache.druid.segment.indexing.granularity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ArbitraryGranularitySpecTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testDefaultQueryGranularity()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(
        null,
        Lists.newArrayList(
            Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
            Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
            Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
            Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
            Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
        )
    );
    Assert.assertNotNull(spec.getQueryGranularity());
  }

  @Test
  public void testBooleanTrigger()
  {
    final List<Interval> inputIntervals = Lists.newArrayList(
        Intervals.of("2023-01-08T00Z/2023-01-11T00Z"),
        Intervals.of("2024-01-01T00Z/20240-01-03T00Z")
    );

    final GranularitySpec safeIntervals = new ArbitraryGranularitySpec(
        Granularities.NONE,
        true,
        true,
        null,
        DateTimes.of("2025-01-01"),
        null
    );

    Assert.assertThrows(IAE.class, () -> safeIntervals.withIntervals(inputIntervals));

    final GranularitySpec unsafeIntervals = new ArbitraryGranularitySpec(
        Granularities.NONE,
        true,
        false,
        null,
        DateTimes.of("2025-01-01"),
        null
    );

    Assert.assertEquals(inputIntervals, unsafeIntervals.withIntervals(inputIntervals).inputIntervals());
  }

  @Test
  public void testNullInterval()
  {
    final List<Interval> invalidLowerLimitIntervals = Collections.singletonList(
        Intervals.of("999-01-08T00Z/2012-01-11T00Z")
    );

    final GranularitySpec nullSpec = new ArbitraryGranularitySpec(
        Granularities.NONE,
        true,
        true,
        null,
        null,
        null
    );

    Assert.assertThrows(IAE.class, () -> nullSpec.withIntervals(invalidLowerLimitIntervals));

    final List<Interval> invalidUpperLimitIntervals = Collections.singletonList(
        new Interval(DateTimes.nowUtc(), DateTimes.nowUtc().plusYears(2))
    );

    Assert.assertThrows(IAE.class, () -> nullSpec.withIntervals(invalidUpperLimitIntervals));

    final List<Interval> inputIntervals = Lists.newArrayList(
        Intervals.of("2024-01-01T00Z/2024-01-11T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    );

    final GranularitySpec spec = nullSpec.withIntervals(inputIntervals);

    Assert.assertEquals(inputIntervals, Lists.newArrayList(spec.inputIntervals()));
  }

  @Test
  public void testIntervalRestrictions()
  {
    final List<Interval> inputIntervals = Collections.singletonList(
        Intervals.of("2024-01-01T00Z/20240-02-01T00Z")
    );

    final GranularitySpec withinRangeSpec = new ArbitraryGranularitySpec(
        Granularities.NONE,
        true,
        true,
        null,
        DateTimes.of("20250-01-01"),
        null
    );
    Assert.assertEquals(inputIntervals, withinRangeSpec.withIntervals(inputIntervals).inputIntervals());

    final GranularitySpec upperLimitReachedSpec = new ArbitraryGranularitySpec(
        Granularities.NONE,
        true,
        true,
        null,
        DateTimes.of("2025-01-01"),
        null
    );
    Assert.assertThrows(IAE.class, () -> upperLimitReachedSpec.withIntervals(inputIntervals));

    final GranularitySpec lowerLimitReachedSpec = new ArbitraryGranularitySpec(
        Granularities.NONE,
        true,
        true,
        DateTimes.of("2025-01-01"),
        DateTimes.of("20250-01-01"),
        null
    );
    Assert.assertThrows(IAE.class, () -> lowerLimitReachedSpec.withIntervals(inputIntervals));

    final GranularitySpec bothLimitsReachedSpec = new ArbitraryGranularitySpec(
        Granularities.NONE,
        true,
        true,
        DateTimes.of("2025-01-01"),
        DateTimes.of("2026-01-01"),
        null
    );

    Assert.assertThrows(IAE.class, () -> bothLimitsReachedSpec.withIntervals(inputIntervals));
  }

  @Test
  public void testSimple()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(
        Granularities.NONE,
        Lists.newArrayList(
            Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
            Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
            Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
            Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
            Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
        )
    );

    Assert.assertTrue(spec.isRollup());

    Assert.assertEquals(
        Lists.newArrayList(
            Intervals.of("2012-01-01T00Z/2012-01-03T00Z"),
            Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
            Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
            Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
            Intervals.of("2012-02-01T00Z/2012-03-01T00Z")
        ),
        Lists.newArrayList(spec.sortedBucketIntervals())
    );

    Assert.assertEquals(
        Optional.of(Intervals.of("2012-01-01T00Z/2012-01-03T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-01T00Z"))
    );

    Assert.assertEquals(
        Optional.of(Intervals.of("2012-01-08T00Z/2012-01-11T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-08T00Z"))
    );

    Assert.assertEquals(
        "2012-01-03T00Z",
        Optional.of(Intervals.of("2012-01-03T00Z/2012-01-04T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-03T00Z"))
    );

    Assert.assertEquals(
        "2012-01-03T01Z",
        Optional.of(Intervals.of("2012-01-03T00Z/2012-01-04T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-03T01Z"))
    );

    Assert.assertEquals(
        "2012-01-04T01Z",
        Optional.<Interval>absent(),
        spec.bucketInterval(DateTimes.of("2012-01-04T01Z"))
    );

    Assert.assertEquals(
        "2012-01-07T23:59:59.999Z",
        Optional.of(Intervals.of("2012-01-07T00Z/2012-01-08T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-07T23:59:59.999Z"))
    );

    Assert.assertEquals(
        "2012-01-08T01Z",
        Optional.of(Intervals.of("2012-01-08T00Z/2012-01-11T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-08T01Z"))
    );

    Assert.assertEquals(
        "2012-01-04T00Z",
        Optional.absent(),
        spec.bucketInterval(DateTimes.of("2012-01-04T00Z"))
    );

    Assert.assertEquals(
        "2012-01-05T00Z",
        Optional.absent(),
        spec.bucketInterval(DateTimes.of("2012-01-05T00Z"))
    );

  }

  @Test
  public void testOverlapViolation()
  {
    List<Interval> intervals = Lists.newArrayList(
        Intervals.of("2012-01-02T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    );

    boolean thrown = false;
    try {
      final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, intervals);
    }
    catch (IllegalArgumentException e) {
      thrown = true;
    }

    Assert.assertTrue("Exception thrown", thrown);
  }

  @Test
  public void testRollupSetting()
  {
    List<Interval> intervals = Lists.newArrayList(
        Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
        Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    );
    final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, false, intervals);

    Assert.assertFalse(spec.isRollup());
  }

  @Test
  public void testOverlapViolationSameStartInstant()
  {
    List<Interval> intervals = Lists.newArrayList(
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-05T00Z")
    );

    boolean thrown = false;
    try {
      final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, intervals);
    }
    catch (IllegalArgumentException e) {
      thrown = true;
    }

    Assert.assertTrue("Exception thrown", thrown);
  }

  @Test
  public void testJson()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, Lists.newArrayList(
        Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
        Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    ));

    try {
      final GranularitySpec rtSpec = JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(spec), GranularitySpec.class);
      Assert.assertEquals(
          "Round-trip",
          ImmutableList.copyOf(spec.sortedBucketIntervals()),
          ImmutableList.copyOf(rtSpec.sortedBucketIntervals())
      );
      Assert.assertEquals(
          "Round-trip",
          ImmutableList.copyOf(spec.inputIntervals()),
          ImmutableList.copyOf(rtSpec.inputIntervals())
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAsMap()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, Lists.newArrayList(
        Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
        Intervals.of("2012-02-01T00Z/2012-03-01T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    ));

    Map<String, Object> map = spec.asMap(JSON_MAPPER);
    final GranularitySpec rtSpec = JSON_MAPPER.convertValue(map, GranularitySpec.class);
    Assert.assertEquals(
        "Round-trip",
        ImmutableList.copyOf(spec.sortedBucketIntervals()),
        ImmutableList.copyOf(rtSpec.sortedBucketIntervals())
    );
    Assert.assertEquals(
        "Round-trip",
        ImmutableList.copyOf(spec.inputIntervals()),
        ImmutableList.copyOf(rtSpec.inputIntervals())
    );
    Assert.assertEquals(spec, rtSpec);
  }

  @Test
  public void testNullInputIntervals()
  {
    final GranularitySpec spec = new ArbitraryGranularitySpec(Granularities.NONE, null);
    Assert.assertFalse(spec.sortedBucketIntervals().iterator().hasNext());
  }
}
