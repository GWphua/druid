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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public abstract class BaseGranularitySpec implements GranularitySpec
{
  public static final Boolean DEFAULT_ROLLUP = Boolean.TRUE;
  public static final Boolean DEFAULT_SAFE = Boolean.FALSE;
  public static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.DAY;
  public static final Granularity DEFAULT_QUERY_GRANULARITY = Granularities.NONE;
  public static final DateTime DEFAULT_SAFE_START = DateTimes.of("1000-01-01T00:00:00+00:00");
  public static final DateTime DEFAULT_SAFE_END = DateTimes.nowUtc().plusYears(1);

  protected final List<Interval> inputIntervals;
  protected final Boolean rollup;
  protected final Boolean safeInput;
  protected final DateTime safeStartTime;
  protected final DateTime safeEndTime;

  public BaseGranularitySpec(
      List<Interval> inputIntervals,
      Boolean rollup,
      Boolean safeInput,
      DateTime safeStartTime,
      DateTime safeEndTime
  )
  {
    this.inputIntervals = inputIntervals == null ? Collections.emptyList() : inputIntervals;
    this.rollup = rollup == null ? DEFAULT_ROLLUP : rollup;
    this.safeInput = safeInput == null ? DEFAULT_SAFE : safeInput;
    this.safeStartTime = safeStartTime == null ? DEFAULT_SAFE_START : safeStartTime;
    this.safeEndTime = safeEndTime == null ? DEFAULT_SAFE_END : safeEndTime;
  }

  @Override
  @JsonProperty("intervals")
  public List<Interval> inputIntervals()
  {
    return inputIntervals;
  }

  @Override
  @JsonProperty("rollup")
  public boolean isRollup()
  {
    return rollup;
  }

  @Override
  @JsonProperty("safeInput")
  public boolean isSafeInput()
  {
    return safeInput;
  }

  @Override
  @JsonProperty("safeStart")
  public DateTime getSafeStart()
  {
    return safeStartTime;
  }

  @Override
  @JsonProperty("safeEnd")
  public DateTime getSafeEnd()
  {
    return safeEndTime;
  }

  @Override
  public Optional<Interval> bucketInterval(DateTime dt)
  {
    return getLookupTableBuckets().bucketInterval(dt);
  }

  @Override
  public TreeSet<Interval> materializedBucketIntervals()
  {
    return getLookupTableBuckets().materializedIntervals();
  }

  protected abstract LookupIntervalBuckets getLookupTableBuckets();

  @Override
  public Map<String, Object> asMap(ObjectMapper objectMapper)
  {
    return objectMapper.convertValue(
        this,
        new TypeReference<Map<String, Object>>()
        {
        }
    );
  }

  /**
   * This is a helper class to facilitate sharing the code for sortedBucketIntervals among
   * the various GranularitySpec implementations. In particular, the UniformGranularitySpec
   * needs to avoid materializing the intervals when the need to traverse them arises.
   */
  protected static class LookupIntervalBuckets
  {
    private final Iterable<Interval> intervalIterable;
    private final TreeSet<Interval> intervals;

    /**
     * @param intervalIterable The intervals to materialize
     */
    public LookupIntervalBuckets(
        Iterable<Interval> intervalIterable,
        Boolean safeInput,
        DateTime safeStartTime,
        DateTime safeEndTime
    )
    {
      this.intervalIterable = intervalIterable;

      if (safeInput && intervalIterable != null) {
        this.checkIntervals(safeStartTime, safeEndTime);
      }

      // The tree set will be materialized on demand (see below) to avoid client code
      // blowing up when constructing this data structure and when the
      // number of intervals is very large...
      this.intervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
    }

    public void checkIntervals(DateTime safeStartTime, DateTime safeEndTime)
    {
      for (Interval i : intervalIterable) {
        if (i.getStart().isBefore(safeStartTime) || i.getEnd().isAfter(safeEndTime)) {
          throw new IAE(
              "Detected invalid interval [%s]. Allowed interval is from [%s] to [%s]",
              i,
              safeStartTime,
              safeEndTime
          );
        }
      }
    }

    /**
     * Returns a bucket interval using a fast lookup into an efficient data structure
     * where all the intervals have been materialized
     *
     * @param dt The date time to lookup
     * @return An Optional containing the interval for the given DateTime if it exists
     */
    public Optional<Interval> bucketInterval(DateTime dt)
    {
      final Interval interval = materializedIntervals().floor(new Interval(dt, DateTimes.MAX));
      if (interval != null && interval.contains(dt)) {
        return Optional.of(interval);
      } else {
        return Optional.absent();
      }
    }

    /**
     * @return An iterator to traverse the materialized intervals. The traversal will be done in
     * order as dictated by Comparators.intervalsByStartThenEnd()
     */
    public Iterator<Interval> iterator()
    {
      return materializedIntervals().iterator();
    }

    /**
     * Helper method to avoid collecting the intervals from the iterator
     *
     * @return The TreeSet of materialized intervals
     */
    public TreeSet<Interval> materializedIntervals()
    {
      if (intervalIterable != null && intervalIterable.iterator().hasNext() && intervals.isEmpty()) {
        Iterators.addAll(intervals, intervalIterable.iterator());
      }
      return intervals;
    }
  }
}
