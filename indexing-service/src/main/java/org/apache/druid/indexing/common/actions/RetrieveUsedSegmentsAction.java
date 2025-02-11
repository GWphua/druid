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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.AbstractBatchSubtask;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Task action to retrieve a collection of segments which have data within the
 * specified intervals and are marked as used.
 * <p>
 * If the task holds REPLACE locks and is writing back to the same datasource,
 * only segments that were created before the REPLACE lock was acquired are
 * returned for an interval. This ensures that the input set of segments for this
 * replace task remains consistent even when new data is appended by other concurrent tasks.
 */
public class RetrieveUsedSegmentsAction implements TaskAction<Collection<DataSegment>>
{
  private static final Logger log = new Logger(RetrieveUsedSegmentsAction.class);

  private final String dataSource;
  private final List<Interval> intervals;
  private final Segments visibility;

  @JsonCreator
  public RetrieveUsedSegmentsAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") Collection<Interval> intervals,
      @JsonProperty("visibility") @Nullable Segments visibility
  )
  {
    if (CollectionUtils.isNullOrEmpty(intervals)) {
      throw InvalidInput.exception("No interval specified for retrieving used segments");
    }

    this.dataSource = dataSource;
    this.intervals = JodaUtils.condenseIntervals(intervals);
    this.visibility = Configs.valueOrDefault(visibility, Segments.ONLY_VISIBLE);
  }

  public RetrieveUsedSegmentsAction(String dataSource, Collection<Interval> intervals)
  {
    this(dataSource, intervals, Segments.ONLY_VISIBLE);
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @JsonProperty
  public Segments getVisibility()
  {
    return visibility;
  }

  @Override
  public TypeReference<Collection<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<>() {};
  }

  @Override
  public Collection<DataSegment> perform(Task task, TaskActionToolbox toolbox)
  {
    // When fetching segments for a datasource other than the one this task is writing to,
    // just return all segments with the needed visibility.
    // This is because we can't ensure that the set of returned segments is consistent throughout the task's lifecycle
    if (!task.getDataSource().equals(dataSource)) {
      return retrieveUsedSegments(toolbox);
    }

    final String supervisorId;
    if (task instanceof AbstractBatchSubtask) {
      supervisorId = ((AbstractBatchSubtask) task).getSupervisorTaskId();
    } else {
      supervisorId = task.getId();
    }

    final Set<ReplaceTaskLock> replaceLocksForTask = toolbox
        .getTaskLockbox()
        .getAllReplaceLocksForDatasource(task.getDataSource())
        .stream()
        .filter(lock -> supervisorId.equals(lock.getSupervisorTaskId()))
        .collect(Collectors.toSet());

    // If there are no replace locks for the task, simply fetch all visible segments for the interval
    if (replaceLocksForTask.isEmpty()) {
      return retrieveUsedSegments(toolbox);
    }

    Map<Interval, Map<String, Set<DataSegment>>> intervalToCreatedToSegments = new HashMap<>();
    for (Pair<DataSegment, String> segmentAndCreatedDate :
        toolbox.getIndexerMetadataStorageCoordinator().retrieveUsedSegmentsAndCreatedDates(dataSource, intervals)) {
      final DataSegment segment = segmentAndCreatedDate.lhs;
      final String createdDate = segmentAndCreatedDate.rhs;
      intervalToCreatedToSegments.computeIfAbsent(segment.getInterval(), s -> new HashMap<>())
                                 .computeIfAbsent(createdDate, c -> new HashSet<>())
                                 .add(segment);
    }

    Set<DataSegment> allSegmentsToBeReplaced = new HashSet<>();
    for (final Map.Entry<Interval, Map<String, Set<DataSegment>>> entry : intervalToCreatedToSegments.entrySet()) {
      final Interval segmentInterval = entry.getKey();
      String lockVersion = null;
      for (ReplaceTaskLock replaceLock : replaceLocksForTask) {
        if (replaceLock.getInterval().contains(segmentInterval)) {
          lockVersion = replaceLock.getVersion();
          break;
        }
      }
      final Map<String, Set<DataSegment>> createdToSegmentsMap = entry.getValue();
      for (Map.Entry<String, Set<DataSegment>> createdAndSegments : createdToSegmentsMap.entrySet()) {
        if (lockVersion == null || lockVersion.compareTo(createdAndSegments.getKey()) > 0) {
          allSegmentsToBeReplaced.addAll(createdAndSegments.getValue());
        } else {
          for (DataSegment segment : createdAndSegments.getValue()) {
            log.info("Ignoring segment[%s] as it has created_date[%s] greater than the REPLACE lock version[%s]",
                     segment.getId(), createdAndSegments.getKey(), lockVersion);
          }
        }
      }
    }

    if (visibility == Segments.ONLY_VISIBLE) {
      return SegmentTimeline.forSegments(allSegmentsToBeReplaced)
                            .findNonOvershadowedObjectsInInterval(Intervals.ETERNITY, Partitions.ONLY_COMPLETE);
    } else {
      return allSegmentsToBeReplaced;
    }
  }

  private Set<DataSegment> retrieveUsedSegments(TaskActionToolbox toolbox)
  {
    return toolbox.getIndexerMetadataStorageCoordinator()
                  .retrieveUsedSegmentsForIntervals(dataSource, intervals, visibility);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RetrieveUsedSegmentsAction that = (RetrieveUsedSegmentsAction) o;

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }
    if (!intervals.equals(that.intervals)) {
      return false;
    }
    return visibility.equals(that.visibility);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, intervals, visibility);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           ", visibility=" + visibility +
           '}';
  }
}
