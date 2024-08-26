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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class UniformGranularitySpecTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testBooleanTrigger()
  {
    final List<Interval> inputIntervals = Lists.newArrayList(
        Intervals.of("2023-01-08T00Z/2023-01-11T00Z"),
        Intervals.of("2024-01-01T00Z/20240-01-03T00Z")
    );

    final GranularitySpec safeIntervals = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.NONE,
        true,
        true,
        null,
        DateTimes.of("2025-01-01"),
        null
    );

    Assert.assertThrows(IAE.class, () -> safeIntervals.withIntervals(inputIntervals));

    final GranularitySpec unsafeIntervals = new UniformGranularitySpec(
        Granularities.DAY,
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

    final GranularitySpec nullSpec = new UniformGranularitySpec(
        Granularities.DAY,
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

    final GranularitySpec withinRangeSpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.NONE,
        true,
        true,
        null,
        DateTimes.of("20250-01-01"),
        null
    );
    Assert.assertEquals(inputIntervals, withinRangeSpec.withIntervals(inputIntervals).inputIntervals());

    final GranularitySpec upperLimitReachedSpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.NONE,
        true,
        true,
        null,
        DateTimes.of("2025-01-01"),
        null
    );
    Assert.assertThrows(IAE.class, () -> upperLimitReachedSpec.withIntervals(inputIntervals));

    final GranularitySpec lowerLimitReachedSpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.NONE,
        true,
        true,
        DateTimes.of("2025-01-01"),
        DateTimes.of("20250-01-01"),
        null
    );
    Assert.assertThrows(IAE.class, () -> lowerLimitReachedSpec.withIntervals(inputIntervals));

    final GranularitySpec bothLimitsReachedSpec = new UniformGranularitySpec(
        Granularities.DAY,
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

    final List<Interval> inputIntervals = Lists.newArrayList(
        Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    );
    final GranularitySpec spec = new UniformGranularitySpec(
        Granularities.DAY,
        null,
        inputIntervals
    );

    Assert.assertTrue(spec.isRollup());

    Assert.assertEquals(
        inputIntervals,
        Lists.newArrayList(spec.inputIntervals())
    );

    Assert.assertEquals(
        Lists.newArrayList(
            Intervals.of("2012-01-01T00Z/P1D"),
            Intervals.of("2012-01-02T00Z/P1D"),
            Intervals.of("2012-01-03T00Z/P1D"),
            Intervals.of("2012-01-07T00Z/P1D"),
            Intervals.of("2012-01-08T00Z/P1D"),
            Intervals.of("2012-01-09T00Z/P1D"),
            Intervals.of("2012-01-10T00Z/P1D")
        ),
        Lists.newArrayList(spec.sortedBucketIntervals())
    );


    Assert.assertEquals(
        Optional.<Interval>absent(),
        spec.bucketInterval(DateTimes.of("2011-01-12T00Z"))
    );

    Assert.assertEquals(
        Optional.of(Intervals.of("2012-01-01T00Z/2012-01-02T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-01T00Z"))
    );

    Assert.assertEquals(
        Optional.of(Intervals.of("2012-01-10T00Z/2012-01-11T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-10T00Z"))
    );

    Assert.assertEquals(
        Optional.<Interval>absent(),
        spec.bucketInterval(DateTimes.of("2012-01-12T00Z"))
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
        Optional.of(Intervals.of("2012-01-08T00Z/2012-01-09T00Z")),
        spec.bucketInterval(DateTimes.of("2012-01-08T01Z"))
    );

  }

  @Test
  public void testRollupSetting()
  {
    List<Interval> intervals = Lists.newArrayList(
        Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    );
    final GranularitySpec spec = new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, false, intervals);

    Assert.assertFalse(spec.isRollup());
  }

  @Test
  public void testJson()
  {
    final GranularitySpec spec = new UniformGranularitySpec(
        Granularities.DAY,
        null,
        Lists.newArrayList(
            Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
            Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
            Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
            Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
        )
    );

    try {
      final GranularitySpec rtSpec = JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(spec), GranularitySpec.class);
      Assert.assertEquals(
          "Round-trip sortedBucketIntervals",
          ImmutableList.copyOf(spec.sortedBucketIntervals()),
          ImmutableList.copyOf(rtSpec.sortedBucketIntervals().iterator())
      );
      Assert.assertEquals(
          "Round-trip granularity",
          spec.getSegmentGranularity(),
          rtSpec.getSegmentGranularity()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAsMap()
  {
    final GranularitySpec spec = new UniformGranularitySpec(
        Granularities.DAY,
        null,
        Lists.newArrayList(
            Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
            Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
            Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
            Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
        )
    );
    Map<String, Object> map = spec.asMap(JSON_MAPPER);
    final GranularitySpec rtSpec = JSON_MAPPER.convertValue(map, GranularitySpec.class);
    Assert.assertEquals(
        "Round-trip sortedBucketIntervals",
        ImmutableList.copyOf(spec.sortedBucketIntervals()),
        ImmutableList.copyOf(rtSpec.sortedBucketIntervals().iterator())
    );
    Assert.assertEquals(
        "Round-trip granularity",
        spec.getSegmentGranularity(),
        rtSpec.getSegmentGranularity()
    );
    Assert.assertEquals(spec, rtSpec);
  }

  @Test
  public void testEquals()
  {

    final GranularitySpec spec = new UniformGranularitySpec(
        Granularities.DAY,
        null,
        Lists.newArrayList(
            Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
            Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
            Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
            Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
        )
    );

    equalsCheck(
        spec,
        new UniformGranularitySpec(
            Granularities.DAY,
            null,
            Lists.newArrayList(
                Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
                Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
                Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
                Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
            )
        )
    );
  }

  public void equalsCheck(GranularitySpec spec1, GranularitySpec spec2)
  {
    Assert.assertEquals(spec1, spec2);
    Assert.assertEquals(spec1.hashCode(), spec2.hashCode());
  }

  @Test
  public void testNotEquals()
  {
    final GranularitySpec spec = new UniformGranularitySpec(
        Granularities.DAY,
        null,
        Lists.newArrayList(
            Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
            Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
            Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
            Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
        )
    );

    notEqualsCheck(
        spec,
        new UniformGranularitySpec(
            Granularities.YEAR,
            null,
            Lists.newArrayList(
                Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
                Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
                Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
                Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
            )
        )
    );
    notEqualsCheck(
        spec,
        new UniformGranularitySpec(
            Granularities.DAY,
            null,
            Lists.newArrayList(
                Intervals.of("2012-01-08T00Z/2012-01-12T00Z"),
                Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
                Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
                Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
            )
        )
    );
    notEqualsCheck(
        spec,
        new UniformGranularitySpec(
            Granularities.DAY,
            Granularities.ALL,
            Lists.newArrayList(
                Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
                Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
                Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
                Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
            )
        )
    );
  }

  @Test
  public void testPeriodSegmentGranularity()
  {
    final GranularitySpec spec = new UniformGranularitySpec(
        new PeriodGranularity(new Period("P1D"), null, DateTimes.inferTzFromString("America/Los_Angeles")),
        null,
        Lists.newArrayList(
            Intervals.of("2012-01-08T00-08:00/2012-01-11T00-08:00"),
            Intervals.of("2012-01-07T00-08:00/2012-01-08T00-08:00"),
            Intervals.of("2012-01-03T00-08:00/2012-01-04T00-08:00"),
            Intervals.of("2012-01-01T00-08:00/2012-01-03T00-08:00"),
            Intervals.of("2012-09-01T00-07:00/2012-09-03T00-07:00")
        )
    );

    Assert.assertTrue(spec.sortedBucketIntervals().iterator().hasNext());

    final Iterable<Interval> intervals = spec.sortedBucketIntervals();
    ArrayList<Long> actualIntervals = new ArrayList<>();
    for (Interval interval : intervals) {
      actualIntervals.add(interval.toDurationMillis());
    }

    final ISOChronology chrono = ISOChronology.getInstance(DateTimes.inferTzFromString("America/Los_Angeles"));

    final ArrayList<Long> expectedIntervals = Lists.newArrayList(
        new Interval("2012-01-01/2012-01-02", chrono).toDurationMillis(),
        new Interval("2012-01-02/2012-01-03", chrono).toDurationMillis(),
        new Interval("2012-01-03/2012-01-04", chrono).toDurationMillis(),
        new Interval("2012-01-07/2012-01-08", chrono).toDurationMillis(),
        new Interval("2012-01-08/2012-01-09", chrono).toDurationMillis(),
        new Interval("2012-01-09/2012-01-10", chrono).toDurationMillis(),
        new Interval("2012-01-10/2012-01-11", chrono).toDurationMillis(),
        new Interval("2012-09-01/2012-09-02", chrono).toDurationMillis(),
        new Interval("2012-09-02/2012-09-03", chrono).toDurationMillis()
    );

    Assert.assertEquals(expectedIntervals, actualIntervals);
  }

  @Test
  public void testUniformGranularitySpecWithLargeNumberOfIntervalsDoesNotBlowUp()
  {
    // just make sure that intervals for uniform spec are not materialized (causing OOM) when created
    final GranularitySpec spec = new UniformGranularitySpec(
        new DurationGranularity(1000, 0),
        null,
        Collections.singletonList(
            Intervals.of("2012-01-01T00Z/P10Y")
        )
    );

    Assert.assertNotNull(spec);

    int count = Iterators.size(spec.sortedBucketIntervals().iterator());
    // account for three leap years...
    Assert.assertEquals(3600 * 24 * 365 * 10 + 3 * 24 * 3600, count);
  }

  private void notEqualsCheck(GranularitySpec spec1, GranularitySpec spec2)
  {
    Assert.assertNotEquals(spec1, spec2);
    Assert.assertNotEquals(spec1.hashCode(), spec2.hashCode());
  }
}
