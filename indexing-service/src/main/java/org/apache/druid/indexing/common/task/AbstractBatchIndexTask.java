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

package org.apache.druid.indexing.common.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.SegmentListUsedAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import org.apache.druid.indexing.firehose.WindowedSegmentId;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Abstract class for batch tasks like {@link IndexTask}.
 * Provides some methods such as {@link #determineSegmentGranularity}, {@link #findInputSegments},
 * and {@link #determineLockGranularityandTryLock} for easily acquiring task locks.
 */
public abstract class AbstractBatchIndexTask extends AbstractTask
{
  private static final Logger log = new Logger(AbstractBatchIndexTask.class);

  private final SegmentLockHelper segmentLockHelper;

  /**
   * State to indicate that this task will use segmentLock or timeChunkLock.
   * This is automatically set when {@link #determineLockGranularityandTryLock} is called.
   */
  private boolean useSegmentLock;

  protected AbstractBatchIndexTask(String id, String dataSource, Map<String, Object> context)
  {
    super(id, dataSource, context);
    segmentLockHelper = new SegmentLockHelper();
  }

  protected AbstractBatchIndexTask(
      String id,
      @Nullable String groupId,
      @Nullable TaskResource taskResource,
      String dataSource,
      @Nullable Map<String, Object> context
  )
  {
    super(id, groupId, taskResource, dataSource, context);
    segmentLockHelper = new SegmentLockHelper();
  }

  /**
   * Return true if this task can overwrite existing segments.
   */
  public abstract boolean requireLockExistingSegments();

  /**
   * Find segments to lock in the given intervals.
   * If this task is intend to overwrite only some segments in those intervals, this method should return only those
   * segments instead of entire segments in those intervals.
   */
  public abstract List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException;

  /**
   * Returns true if this task is in the perfect (guaranteed) rollup mode.
   */
  public abstract boolean isPerfectRollup();

  /**
   * Returns the segmentGranularity defined in the ingestion spec.
   */
  @Nullable
  public abstract Granularity getSegmentGranularity();

  public boolean isUseSegmentLock()
  {
    return useSegmentLock;
  }

  public SegmentLockHelper getSegmentLockHelper()
  {
    return segmentLockHelper;
  }

  /**
   * Determine lockGranularity to use and try to acquire necessary locks.
   * This method respects the value of 'forceTimeChunkLock' in task context.
   * If it's set to false or missing, this method checks if this task can use segmentLock.
   */
  protected boolean determineLockGranularityAndTryLock(
      TaskActionClient client,
      GranularitySpec granularitySpec
  ) throws IOException
  {
    final List<Interval> intervals = granularitySpec.bucketIntervals().isPresent()
                                     ? new ArrayList<>(granularitySpec.bucketIntervals().get())
                                     : Collections.emptyList();
    return determineLockGranularityandTryLock(client, intervals);
  }

  boolean determineLockGranularityandTryLock(TaskActionClient client, List<Interval> intervals) throws IOException
  {
    final boolean forceTimeChunkLock = getContextValue(
        Tasks.FORCE_TIME_CHUNK_LOCK_KEY,
        Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK
    );
    // Respect task context value most.
    if (forceTimeChunkLock) {
      log.info("[%s] is set to true in task context. Use timeChunk lock", Tasks.FORCE_TIME_CHUNK_LOCK_KEY);
      useSegmentLock = false;
      if (!intervals.isEmpty()) {
        return tryTimeChunkLock(client, intervals);
      } else {
        return true;
      }
    } else {
      if (!intervals.isEmpty()) {
        final LockGranularityDetermineResult result = determineSegmentGranularity(client, intervals);
        useSegmentLock = result.lockGranularity == LockGranularity.SEGMENT;
        return tryLockWithDetermineResult(client, result);
      } else {
        return true;
      }
    }
  }

  boolean determineLockGranularityandTryLockWithSegments(TaskActionClient client, List<DataSegment> segments)
      throws IOException
  {
    final boolean forceTimeChunkLock = getContextValue(
        Tasks.FORCE_TIME_CHUNK_LOCK_KEY,
        Tasks.DEFAULT_FORCE_TIME_CHUNK_LOCK
    );
    if (forceTimeChunkLock) {
      log.info("[%s] is set to true in task context. Use timeChunk lock", Tasks.FORCE_TIME_CHUNK_LOCK_KEY);
      useSegmentLock = false;
      return tryTimeChunkLock(
          client,
          new ArrayList<>(segments.stream().map(DataSegment::getInterval).collect(Collectors.toSet()))
      );
    } else {
      final LockGranularityDetermineResult result = determineSegmentGranularity(segments);
      useSegmentLock = result.lockGranularity == LockGranularity.SEGMENT;
      return tryLockWithDetermineResult(client, result);
    }
  }

  private LockGranularityDetermineResult determineSegmentGranularity(TaskActionClient client, List<Interval> intervals)
      throws IOException
  {
    if (requireLockExistingSegments()) {
      if (isPerfectRollup()) {
        log.info("Using timeChunk lock for perfect rollup");
        return new LockGranularityDetermineResult(LockGranularity.TIME_CHUNK, intervals, null);
      } else if (!intervals.isEmpty()) {
        // This method finds segments falling in all given intervals and then tries to lock those segments.
        // Thus, there might be a race between calling findSegmentsToLock() and determineSegmentGranularity(),
        // i.e., a new segment can be added to the interval or an existing segment might be removed.
        // Removed segments should be fine because indexing tasks would do nothing with removed segments.
        // However, tasks wouldn't know about new segments added after findSegmentsToLock() call, it may missing those
        // segments. This is usually fine, but if you want to avoid this, you should use timeChunk lock instead.
        return determineSegmentGranularity(findSegmentsToLock(client, intervals));
      } else {
        log.info("Using segment lock for empty intervals");
        return new LockGranularityDetermineResult(LockGranularity.SEGMENT, null, Collections.emptyList());
      }
    } else {
      log.info("Using segment lock since we don't have to lock existing segments");
      return new LockGranularityDetermineResult(LockGranularity.SEGMENT, null, Collections.emptyList());
    }
  }

  private boolean tryLockWithDetermineResult(TaskActionClient client, LockGranularityDetermineResult result)
      throws IOException
  {
    if (result.lockGranularity == LockGranularity.TIME_CHUNK) {
      return tryTimeChunkLock(client, Preconditions.checkNotNull(result.intervals, "intervals"));
    } else {
      return segmentLockHelper.verifyAndLockExistingSegments(
          client,
          Preconditions.checkNotNull(result.segments, "segments")
      );
    }
  }

  private boolean tryTimeChunkLock(TaskActionClient client, List<Interval> intervals) throws IOException
  {
    // In this case, the intervals to lock must be aligned with segmentGranularity if it's defined
    final Set<Interval> uniqueIntervals = new HashSet<>();
    for (Interval interval : JodaUtils.condenseIntervals(intervals)) {
      final Granularity segmentGranularity = getSegmentGranularity();
      if (segmentGranularity == null) {
        uniqueIntervals.add(interval);
      } else {
        Iterables.addAll(uniqueIntervals, segmentGranularity.getIterable(interval));
      }
    }

    for (Interval interval : uniqueIntervals) {
      final TaskLock lock = client.submit(new TimeChunkLockTryAcquireAction(TaskLockType.EXCLUSIVE, interval));
      if (lock == null) {
        return false;
      }
    }
    return true;
  }

  private LockGranularityDetermineResult determineSegmentGranularity(List<DataSegment> segments)
  {
    if (segments.isEmpty()) {
      log.info("Using segment lock for empty segments");
      // Set useSegmentLock even though we don't get any locks.
      // Note that we should get any lock before data ingestion if we are supposed to use timChunk lock.
      return new LockGranularityDetermineResult(LockGranularity.SEGMENT, null, Collections.emptyList());
    }

    if (requireLockExistingSegments()) {
      final Granularity granularityFromSegments = findGranularityFromSegments(segments);
      @Nullable
      final Granularity segmentGranularityFromSpec = getSegmentGranularity();
      final List<Interval> intervals = segments.stream().map(DataSegment::getInterval).collect(Collectors.toList());

      if (granularityFromSegments == null
          || segmentGranularityFromSpec != null
             && (!granularityFromSegments.equals(segmentGranularityFromSpec)
                 || segments.stream().anyMatch(segment -> !segmentGranularityFromSpec.isAligned(segment.getInterval())))) {
        // This case is one of the followings:
        // 1) Segments have different granularities.
        // 2) Segment granularity in ingestion spec is different from the one of existig segments.
        // 3) Some existing segments are not aligned with the segment granularity in the ingestion spec.
        log.info("Detected segmentGranularity change. Using timeChunk lock");
        return new LockGranularityDetermineResult(LockGranularity.TIME_CHUNK, intervals, null);
      } else {
        // Use segment lock
        // Create a timeline to find latest segments only
        final VersionedIntervalTimeline<String, DataSegment> timeline = VersionedIntervalTimeline.forSegments(
            segments
        );

        final List<DataSegment> segmentsToLock = timeline
            .lookup(JodaUtils.umbrellaInterval(intervals))
            .stream()
            .map(TimelineObjectHolder::getObject)
            .flatMap(partitionHolder -> StreamSupport.stream(partitionHolder.spliterator(), false))
            .map(PartitionChunk::getObject)
            .collect(Collectors.toList());
        log.info("No segmentGranularity change detected and it's not perfect rollup. Using segment lock");
        return new LockGranularityDetermineResult(LockGranularity.SEGMENT, null, segmentsToLock);
      }
    } else {
      // Set useSegmentLock even though we don't get any locks.
      // Note that we should get any lock before data ingestion if we are supposed to use timChunk lock.
      log.info("Using segment lock since we don't have to lock existing segments");
      return new LockGranularityDetermineResult(LockGranularity.SEGMENT, null, Collections.emptyList());
    }
  }

  @Nullable
  static Granularity findGranularityFromSegments(List<DataSegment> segments)
  {
    if (segments.isEmpty()) {
      return null;
    }
    final Period firstSegmentPeriod = segments.get(0).getInterval().toPeriod();
    final boolean allHasSameGranularity = segments
        .stream()
        .allMatch(segment -> firstSegmentPeriod.equals(segment.getInterval().toPeriod()));
    if (allHasSameGranularity) {
      return GranularityType.fromPeriod(firstSegmentPeriod).getDefaultGranularity();
    } else {
      return null;
    }
  }

  /**
   * If the given firehoseFactory is {@link IngestSegmentFirehoseFactory}, then it finds the segments to lock
   * from the firehoseFactory. This is because those segments will be read by this task no matter what segments would be
   * filtered by intervalsToRead, so they need to be locked.
   *
   * However, firehoseFactory is not IngestSegmentFirehoseFactory, it means this task will overwrite some segments
   * with data read from some input source outside of Druid. As a result, only the segments falling in intervalsToRead
   * should be locked.
   */
  protected static List<DataSegment> findInputSegments(
      String dataSource,
      TaskActionClient actionClient,
      List<Interval> intervalsToRead,
      FirehoseFactory firehoseFactory
  ) throws IOException
  {
    if (firehoseFactory instanceof IngestSegmentFirehoseFactory) {
      // intervalsToRead is ignored here.
      final List<WindowedSegmentId> inputSegments = ((IngestSegmentFirehoseFactory) firehoseFactory).getSegments();
      if (inputSegments == null) {
        final Interval inputInterval = Preconditions.checkNotNull(
            ((IngestSegmentFirehoseFactory) firehoseFactory).getInterval(),
            "input interval"
        );

        return actionClient.submit(
            new SegmentListUsedAction(dataSource, null, Collections.singletonList(inputInterval))
        );
      } else {
        final List<String> inputSegmentIds = inputSegments.stream()
                                                          .map(WindowedSegmentId::getSegmentId)
                                                          .collect(Collectors.toList());
        final List<DataSegment> dataSegmentsInIntervals = actionClient.submit(
            new SegmentListUsedAction(
                dataSource,
                null,
                inputSegments.stream()
                             .flatMap(windowedSegmentId -> windowedSegmentId.getIntervals().stream())
                             .collect(Collectors.toSet())
            )
        );
        return dataSegmentsInIntervals.stream()
                                      .filter(segment -> inputSegmentIds.contains(segment.getId().toString()))
                                      .collect(Collectors.toList());
      }
    } else {
      return actionClient.submit(new SegmentListUsedAction(dataSource, null, intervalsToRead));
    }
  }

  private static class LockGranularityDetermineResult
  {
    private final LockGranularity lockGranularity;
    @Nullable
    private final List<Interval> intervals; // null for segmentLock
    @Nullable
    private final List<DataSegment> segments; // null for timeChunkLock

    private LockGranularityDetermineResult(
        LockGranularity lockGranularity,
        @Nullable List<Interval> intervals,
        @Nullable List<DataSegment> segments
    )
    {
      this.lockGranularity = lockGranularity;
      this.intervals = intervals;
      this.segments = segments;
    }
  }
}
