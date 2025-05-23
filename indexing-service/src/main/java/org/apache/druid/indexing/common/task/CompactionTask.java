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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.Property;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.indexing.CombinedDataSchema;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The client representation of this task is {@link ClientCompactionTaskQuery}. JSON
 * serialization fields of this class must correspond to those of {@link
 * ClientCompactionTaskQuery}.
 */
public class CompactionTask extends AbstractBatchIndexTask implements PendingSegmentAllocatingTask
{
  public static final String TYPE = "compact";
  private static final Logger log = new Logger(CompactionTask.class);

  /**
   * The CompactionTask creates and runs multiple IndexTask instances. When the {@link AppenderatorsManager}
   * is asked to clean up, it does so on a per-task basis keyed by task ID. However, the subtask IDs of the
   * CompactionTask are not externally visible. This context flag is used to ensure that all the appenderators
   * created for the CompactionTasks's subtasks are tracked under the ID of the parent CompactionTask.
   * The CompactionTask may change in the future and no longer require this behavior (e.g., reusing the same
   * Appenderator across subtasks, or allowing the subtasks to use the same ID). The CompactionTask is also the only
   * task type that currently creates multiple appenderators. Thus, a context flag is used to handle this case
   * instead of a more general approach such as new methods on the Task interface.
   */
  public static final String CTX_KEY_APPENDERATOR_TRACKING_TASK_ID = "appenderatorTrackingTaskId";

  static {
    Verify.verify(TYPE.equals(CompactSegments.COMPACTION_TASK_TYPE));
  }

  private final CompactionIOConfig ioConfig;
  @Nullable
  private final DimensionsSpec dimensionsSpec;
  @Nullable
  private final CompactionTransformSpec transformSpec;
  @Nullable
  private final AggregatorFactory[] metricsSpec;
  @Nullable
  private final ClientCompactionTaskGranularitySpec granularitySpec;
  @Nullable
  private final List<AggregateProjectionSpec> projections;
  @Nullable
  private final CompactionTuningConfig tuningConfig;
  @Nullable
  private final CompactionRunner compactionRunner;
  @JsonIgnore
  private final SegmentProvider segmentProvider;
  @JsonIgnore
  private final CurrentSubTaskHolder currentSubTaskHolder;

  @JsonCreator
  public CompactionTask(
      @JsonProperty("id") @Nullable final String id,
      @JsonProperty("resource") @Nullable final TaskResource taskResource,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") @Deprecated @Nullable final Interval interval,
      @JsonProperty("segments") @Deprecated @Nullable final List<DataSegment> segments,
      @JsonProperty("ioConfig") @Nullable CompactionIOConfig ioConfig,
      @JsonProperty("dimensions") @Nullable final DimensionsSpec dimensions,
      @JsonProperty("dimensionsSpec") @Nullable final DimensionsSpec dimensionsSpec,
      @JsonProperty("transformSpec") @Nullable final CompactionTransformSpec transformSpec,
      @JsonProperty("metricsSpec") @Nullable final AggregatorFactory[] metricsSpec,
      @JsonProperty("segmentGranularity") @Deprecated @Nullable final Granularity segmentGranularity,
      @JsonProperty("granularitySpec") @Nullable final ClientCompactionTaskGranularitySpec granularitySpec,
      @JsonProperty("projections") @Nullable List<AggregateProjectionSpec> projections,
      @JsonProperty("tuningConfig") @Nullable final TuningConfig tuningConfig,
      @JsonProperty("context") @Nullable final Map<String, Object> context,
      @JsonProperty("compactionRunner") final CompactionRunner compactionRunner,
      @JacksonInject SegmentCacheManagerFactory segmentCacheManagerFactory
  )
  {
    super(
        getOrMakeId(id, TYPE, dataSource),
        null,
        taskResource,
        dataSource,
        context,
        -1,
        computeCompactionIngestionMode(ioConfig)
    );
    Checks.checkOneNotNullOrEmpty(
        ImmutableList.of(
            new Property<>("ioConfig", ioConfig),
            new Property<>("interval", interval),
            new Property<>("segments", segments)
        )
    );
    if (ioConfig != null) {
      this.ioConfig = ioConfig;
    } else if (interval != null) {
      this.ioConfig = new CompactionIOConfig(new CompactionIntervalSpec(interval, null), false, null);
    } else {
      // We already checked segments is not null or empty above.
      //noinspection ConstantConditions
      this.ioConfig = new CompactionIOConfig(SpecificSegmentsSpec.fromSegments(segments), false, null);
    }
    this.dimensionsSpec = dimensionsSpec == null ? dimensions : dimensionsSpec;
    this.transformSpec = transformSpec;
    this.metricsSpec = metricsSpec;
    // Prior to apache/druid#10843 users could specify segmentGranularity using `segmentGranularity`
    // Now users should prefer to use `granularitySpec`
    // In case users accidentally specify both, and they are conflicting, warn the user instead of proceeding
    // by picking one or another.
    if (granularitySpec != null
        && segmentGranularity != null
        && !segmentGranularity.equals(granularitySpec.getSegmentGranularity())) {
      throw new IAE(StringUtils.format(
          "Conflicting segment granularities found %s(segmentGranularity) and %s(granularitySpec.segmentGranularity).\n"
          + "Remove `segmentGranularity` and set the `granularitySpec.segmentGranularity` to the expected granularity",
          segmentGranularity,
          granularitySpec.getSegmentGranularity()
      ));
    }
    if (granularitySpec == null && segmentGranularity != null) {
      this.granularitySpec = new ClientCompactionTaskGranularitySpec(segmentGranularity, null, null);
    } else {
      this.granularitySpec = granularitySpec;
    }
    this.projections = projections;
    this.tuningConfig = tuningConfig != null ? getTuningConfig(tuningConfig) : null;
    this.segmentProvider = new SegmentProvider(dataSource, this.ioConfig.getInputSpec());
    // Note: The default compactionRunnerType used here should match the default runner used in CompactSegments#run
    // when no runner is detected in the returned compactionTaskQuery.
    this.compactionRunner = compactionRunner == null
                            ? new NativeCompactionRunner(segmentCacheManagerFactory)
                            : compactionRunner;
    this.currentSubTaskHolder = this.compactionRunner.getCurrentSubTaskHolder();

    // Do not load any lookups in sub-tasks launched by compaction task, unless transformSpec is present.
    // If transformSpec is present, we will not modify the context so that the sub-tasks can make the
    // decision based on context values, loading all lookups by default.
    // This is done to ensure backward compatibility since transformSpec can reference lookups.
    if (transformSpec == null) {
      addToContextIfAbsent(LookupLoadingSpec.CTX_LOOKUP_LOADING_MODE, LookupLoadingSpec.Mode.NONE.toString());
    }
  }

  @VisibleForTesting
  static CompactionTuningConfig getTuningConfig(TuningConfig tuningConfig)
  {
    if (tuningConfig instanceof CompactionTuningConfig) {
      return (CompactionTuningConfig) tuningConfig;
    } else if (tuningConfig instanceof ParallelIndexTuningConfig) {
      final ParallelIndexTuningConfig parallelIndexTuningConfig = (ParallelIndexTuningConfig) tuningConfig;
      return new CompactionTuningConfig(
          null,
          parallelIndexTuningConfig.getMaxRowsPerSegment(),
          parallelIndexTuningConfig.getAppendableIndexSpec(),
          parallelIndexTuningConfig.getMaxRowsInMemory(),
          parallelIndexTuningConfig.getMaxBytesInMemory(),
          parallelIndexTuningConfig.isSkipBytesInMemoryOverheadCheck(),
          parallelIndexTuningConfig.getMaxTotalRows(),
          parallelIndexTuningConfig.getNumShards(),
          parallelIndexTuningConfig.getSplitHintSpec(),
          parallelIndexTuningConfig.getPartitionsSpec(),
          parallelIndexTuningConfig.getIndexSpec(),
          parallelIndexTuningConfig.getIndexSpecForIntermediatePersists(),
          parallelIndexTuningConfig.getMaxPendingPersists(),
          parallelIndexTuningConfig.isForceGuaranteedRollup(),
          parallelIndexTuningConfig.isReportParseExceptions(),
          parallelIndexTuningConfig.getPushTimeout(),
          parallelIndexTuningConfig.getSegmentWriteOutMediumFactory(),
          null,
          parallelIndexTuningConfig.getMaxNumConcurrentSubTasks(),
          parallelIndexTuningConfig.getMaxRetry(),
          parallelIndexTuningConfig.getTaskStatusCheckPeriodMs(),
          parallelIndexTuningConfig.getChatHandlerTimeout(),
          parallelIndexTuningConfig.getChatHandlerNumRetries(),
          parallelIndexTuningConfig.getMaxNumSegmentsToMerge(),
          parallelIndexTuningConfig.getTotalNumMergeTasks(),
          parallelIndexTuningConfig.isLogParseExceptions(),
          parallelIndexTuningConfig.getMaxParseExceptions(),
          parallelIndexTuningConfig.getMaxSavedParseExceptions(),
          parallelIndexTuningConfig.getMaxColumnsToMerge(),
          parallelIndexTuningConfig.getAwaitSegmentAvailabilityTimeoutMillis(),
          parallelIndexTuningConfig.getNumPersistThreads()
      );
    } else if (tuningConfig instanceof IndexTuningConfig) {
      final IndexTuningConfig indexTuningConfig = (IndexTuningConfig) tuningConfig;
      return new CompactionTuningConfig(
          null,
          indexTuningConfig.getMaxRowsPerSegment(),
          indexTuningConfig.getAppendableIndexSpec(),
          indexTuningConfig.getMaxRowsInMemory(),
          indexTuningConfig.getMaxBytesInMemory(),
          indexTuningConfig.isSkipBytesInMemoryOverheadCheck(),
          indexTuningConfig.getMaxTotalRows(),
          indexTuningConfig.getNumShards(),
          null,
          indexTuningConfig.getPartitionsSpec(),
          indexTuningConfig.getIndexSpec(),
          indexTuningConfig.getIndexSpecForIntermediatePersists(),
          indexTuningConfig.getMaxPendingPersists(),
          indexTuningConfig.isForceGuaranteedRollup(),
          indexTuningConfig.isReportParseExceptions(),
          indexTuningConfig.getPushTimeout(),
          indexTuningConfig.getSegmentWriteOutMediumFactory(),
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          indexTuningConfig.isLogParseExceptions(),
          indexTuningConfig.getMaxParseExceptions(),
          indexTuningConfig.getMaxSavedParseExceptions(),
          indexTuningConfig.getMaxColumnsToMerge(),
          indexTuningConfig.getAwaitSegmentAvailabilityTimeoutMillis(),
          indexTuningConfig.getNumPersistThreads()
      );
    } else {
      throw new ISE(
          "Unknown tuningConfig type: [%s], Must be in [%s, %s, %s]",
          tuningConfig.getClass().getName(),
          CompactionTuningConfig.class.getName(),
          ParallelIndexTuningConfig.class.getName(),
          IndexTuningConfig.class.getName()
      );
    }
  }

  @VisibleForTesting
  public CurrentSubTaskHolder getCurrentSubTaskHolder()
  {
    return currentSubTaskHolder;
  }

  @JsonProperty
  public CompactionIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @JsonProperty
  @Nullable
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty
  @Nullable
  public CompactionTransformSpec getTransformSpec()
  {
    return transformSpec;
  }

  @JsonProperty
  @Nullable
  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
  }

  @JsonInclude(Include.NON_NULL)
  @JsonProperty
  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    return granularitySpec == null ? null : granularitySpec.getSegmentGranularity();
  }

  @JsonProperty
  @Nullable
  public ClientCompactionTaskGranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @Nullable
  @JsonProperty
  public CompactionTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  public List<AggregateProjectionSpec> getProjections()
  {
    return projections;
  }

  @JsonProperty
  public CompactionRunner getCompactionRunner()
  {
    return compactionRunner;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public String getTaskAllocatorId()
  {
    return getGroupId();
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    return ImmutableSet.of();
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_MERGE_TASK_PRIORITY);
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    final List<DataSegment> segments = segmentProvider.findSegments(taskActionClient);
    return determineLockGranularityAndTryLockWithSegments(taskActionClient, segments, segmentProvider::checkSegments);
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    return true;
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException
  {
    return ImmutableList.copyOf(
        taskActionClient.submit(new RetrieveUsedSegmentsAction(getDataSource(), intervals))
    );
  }

  @Override
  public boolean isPerfectRollup()
  {
    return tuningConfig != null && tuningConfig.isForceGuaranteedRollup();
  }

  /**
   * Checks if multi-valued string dimensions need to be analyzed by downloading the segments.
   * This method returns true only for MSQ engine when either of the following holds true:
   * <ul>
   * <li> Range partitioning is done on a string dimension or an unknown dimension
   * (since MSQ does not support partitioning on a multi-valued string dimension) </li>
   * <li> Rollup is done on a string dimension or an unknown dimension
   * (since MSQ requires multi-valued string dimensions to be converted to arrays for rollup) </li>
   * </ul>
   * @return false for native engine, true for MSQ engine only when partitioning or rollup is done on a string
   * or unknown dimension.
   */
  boolean identifyMultiValuedDimensions()
  {
    if (compactionRunner instanceof NativeCompactionRunner) {
      return false;
    }
    // Rollup can be true even when granularitySpec is not known since rollup is then decided based on segment analysis
    final boolean isPossiblyRollup = granularitySpec == null || !Boolean.FALSE.equals(granularitySpec.isRollup());
    boolean isRangePartitioned = tuningConfig != null
                                 && tuningConfig.getPartitionsSpec() instanceof DimensionRangePartitionsSpec;

    if (dimensionsSpec == null || dimensionsSpec.getDimensions().isEmpty()) {
      return isPossiblyRollup || isRangePartitioned;
    } else {
      boolean isRollupOnStringDimension = isPossiblyRollup &&
                                          dimensionsSpec.getDimensions()
                                                        .stream()
                                                        .anyMatch(dim -> ColumnType.STRING.equals(dim.getColumnType()));

      boolean isPartitionedOnStringDimension =
          isRangePartitioned &&
          dimensionsSpec.getDimensions()
                        .stream()
                        .anyMatch(
                            dim -> ColumnType.STRING.equals(dim.getColumnType())
                                   && ((DimensionRangePartitionsSpec) tuningConfig.getPartitionsSpec())
                                       .getPartitionDimensions()
                                       .contains(dim.getName())
                        );
      return isRollupOnStringDimension || isPartitionedOnStringDimension;
    }
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    emitMetric(toolbox.getEmitter(), "ingest/count", 1);

    final Map<Interval, DataSchema> intervalDataSchemas = createDataSchemasForIntervals(
        toolbox,
        getTaskLockHelper().getLockGranularityToUse(),
        segmentProvider,
        dimensionsSpec,
        transformSpec,
        metricsSpec,
        granularitySpec,
        projections,
        getMetricBuilder(),
        this.identifyMultiValuedDimensions()
    );

    registerResourceCloserOnAbnormalExit(compactionRunner.getCurrentSubTaskHolder());
    CompactionConfigValidationResult supportsCompactionConfig = compactionRunner.validateCompactionTask(
        this,
        intervalDataSchemas
    );
    if (!supportsCompactionConfig.isValid()) {
      throw InvalidInput.exception("Compaction spec not supported. Reason[%s].", supportsCompactionConfig.getReason());
    }
    return compactionRunner.runCompactionTasks(this, intervalDataSchemas, toolbox);
  }

  /**
   * Generate dataschema for segments in each interval.
   *
   * @throws IOException if an exception occurs whie retrieving used segments to
   *                     determine schemas.
   */
  @VisibleForTesting
  static Map<Interval, DataSchema> createDataSchemasForIntervals(
      final TaskToolbox toolbox,
      final LockGranularity lockGranularityInUse,
      final SegmentProvider segmentProvider,
      @Nullable final DimensionsSpec dimensionsSpec,
      @Nullable final CompactionTransformSpec transformSpec,
      @Nullable final AggregatorFactory[] metricsSpec,
      @Nullable final ClientCompactionTaskGranularitySpec granularitySpec,
      @Nullable final List<AggregateProjectionSpec> projections,
      final ServiceMetricEvent.Builder metricBuilder,
      boolean needMultiValuedColumns
  ) throws IOException
  {
    final Iterable<DataSegment> timelineSegments = retrieveRelevantTimelineHolders(
        toolbox,
        segmentProvider,
        lockGranularityInUse
    );

    if (!timelineSegments.iterator().hasNext()) {
      return Collections.emptyMap();
    }

    if (granularitySpec == null || granularitySpec.getSegmentGranularity() == null) {
      Map<Interval, DataSchema> intervalDataSchemaMap = new HashMap<>();

      // original granularity
      final Map<Interval, List<DataSegment>> intervalToSegments = new TreeMap<>(
          Comparators.intervalsByStartThenEnd()
      );

      for (final DataSegment dataSegment : timelineSegments) {
        intervalToSegments.computeIfAbsent(dataSegment.getInterval(), k -> new ArrayList<>())
                          .add(dataSegment);
      }

      // unify overlapping intervals to ensure overlapping segments compacting in the same indexSpec
      List<NonnullPair<Interval, List<DataSegment>>> intervalToSegmentsUnified = new ArrayList<>();
      Interval union = null;
      List<DataSegment> segments = new ArrayList<>();
      for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
        Interval cur = entry.getKey();
        if (union == null) {
          union = cur;
          segments.addAll(entry.getValue());
        } else if (union.overlaps(cur)) {
          union = Intervals.utc(union.getStartMillis(), Math.max(union.getEndMillis(), cur.getEndMillis()));
          segments.addAll(entry.getValue());
        } else {
          intervalToSegmentsUnified.add(new NonnullPair<>(union, segments));
          union = cur;
          segments = new ArrayList<>(entry.getValue());
        }
      }

      intervalToSegmentsUnified.add(new NonnullPair<>(union, segments));

      for (NonnullPair<Interval, List<DataSegment>> entry : intervalToSegmentsUnified) {
        final Interval interval = entry.lhs;
        final List<DataSegment> segmentsToCompact = entry.rhs;
        // If granularitySpec is not null, then set segmentGranularity. Otherwise,
        // creates new granularitySpec and set segmentGranularity
        Granularity segmentGranularityToUse = GranularityType.fromPeriod(interval.toPeriod()).getDefaultGranularity();
        final DataSchema dataSchema = createDataSchema(
            toolbox.getEmitter(),
            metricBuilder,
            segmentProvider.dataSource,
            interval,
            lazyFetchSegments(segmentsToCompact, toolbox.getSegmentCacheManager(), toolbox.getIndexIO()),
            dimensionsSpec,
            transformSpec,
            metricsSpec,
            granularitySpec == null
            ? new ClientCompactionTaskGranularitySpec(segmentGranularityToUse, null, null)
            : granularitySpec.withSegmentGranularity(segmentGranularityToUse),
            projections,
            needMultiValuedColumns
        );
        intervalDataSchemaMap.put(interval, dataSchema);
      }
      return intervalDataSchemaMap;
    } else {
      // given segment granularity
      final DataSchema dataSchema = createDataSchema(
          toolbox.getEmitter(),
          metricBuilder,
          segmentProvider.dataSource,
          JodaUtils.umbrellaInterval(
              Iterables.transform(
                  timelineSegments,
                  DataSegment::getInterval
              )
          ),
          lazyFetchSegments(
              timelineSegments,
              toolbox.getSegmentCacheManager(),
              toolbox.getIndexIO()
          ),
          dimensionsSpec,
          transformSpec,
          metricsSpec,
          granularitySpec,
          projections,
          needMultiValuedColumns
      );
      return Collections.singletonMap(segmentProvider.interval, dataSchema);
    }
  }

  private static Iterable<DataSegment> retrieveRelevantTimelineHolders(
      TaskToolbox toolbox,
      SegmentProvider segmentProvider,
      LockGranularity lockGranularityInUse
  ) throws IOException
  {
    final List<DataSegment> usedSegments =
        segmentProvider.findSegments(toolbox.getTaskActionClient());
    segmentProvider.checkSegments(lockGranularityInUse, usedSegments);
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = SegmentTimeline
        .forSegments(usedSegments)
        .lookup(segmentProvider.interval);
    return VersionedIntervalTimeline.getAllObjects(timelineSegments);
  }

  private static DataSchema createDataSchema(
      ServiceEmitter emitter,
      ServiceMetricEvent.Builder metricBuilder,
      String dataSource,
      Interval totalInterval,
      Iterable<Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>>> segments,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable CompactionTransformSpec transformSpec,
      @Nullable AggregatorFactory[] metricsSpec,
      @Nonnull ClientCompactionTaskGranularitySpec granularitySpec,
      @Nullable List<AggregateProjectionSpec> projections,
      boolean needMultiValuedColumns
  )
  {
    // Check index metadata & decide which values to propagate (i.e. carry over) for rollup & queryGranularity
    final ExistingSegmentAnalyzer existingSegmentAnalyzer = new ExistingSegmentAnalyzer(
        segments,
        granularitySpec.isRollup() == null,
        granularitySpec.getQueryGranularity() == null,
        dimensionsSpec == null,
        metricsSpec == null,
        projections == null,
        needMultiValuedColumns
    );

    final Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      existingSegmentAnalyzer.fetchAndProcessIfNeeded();
    }
    finally {
      if (emitter != null) {
        emitter.emit(
            metricBuilder.setMetric(
                "compact/segmentAnalyzer/fetchAndProcessMillis",
                stopwatch.millisElapsed()
            )
        );
      }
    }

    final Granularity queryGranularityToUse;
    if (granularitySpec.getQueryGranularity() == null) {
      queryGranularityToUse = existingSegmentAnalyzer.getQueryGranularity();
      log.info("Generate compaction task spec with segments original query granularity[%s]", queryGranularityToUse);
    } else {
      queryGranularityToUse = granularitySpec.getQueryGranularity();
      log.info(
          "Generate compaction task spec with new query granularity overrided from input[%s].",
          queryGranularityToUse
      );
    }

    final GranularitySpec uniformGranularitySpec = new UniformGranularitySpec(
        Preconditions.checkNotNull(granularitySpec.getSegmentGranularity()),
        queryGranularityToUse,
        granularitySpec.isRollup() == null ? existingSegmentAnalyzer.getRollup() : granularitySpec.isRollup(),
        Collections.singletonList(totalInterval)
    );

    // find unique dimensions
    final DimensionsSpec finalDimensionsSpec;
    if (dimensionsSpec == null) {
      finalDimensionsSpec = existingSegmentAnalyzer.getDimensionsSpec();
    } else {
      finalDimensionsSpec = dimensionsSpec;
    }

    final AggregatorFactory[] finalMetricsSpec;
    if (metricsSpec == null) {
      finalMetricsSpec = existingSegmentAnalyzer.getMetricsSpec();
    } else {
      finalMetricsSpec = metricsSpec;
    }
    final List<AggregateProjectionSpec> projectionSpecs;
    if (projections != null) {
      projectionSpecs = projections;
    } else {
      projectionSpecs = existingSegmentAnalyzer.getProjections();
    }

    return new CombinedDataSchema(
        dataSource,
        new TimestampSpec(ColumnHolder.TIME_COLUMN_NAME, "millis", null),
        finalDimensionsSpec,
        finalMetricsSpec,
        uniformGranularitySpec,
        transformSpec == null ? null : new TransformSpec(transformSpec.getFilter(), null),
        projectionSpecs,
        existingSegmentAnalyzer.getMultiValuedDimensions()
    );
  }

  /**
   * Lazily fetch and load {@link QueryableIndex}, skipping tombstones.
   */
  private static Iterable<Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>>> lazyFetchSegments(
      Iterable<DataSegment> dataSegments,
      SegmentCacheManager segmentCacheManager,
      IndexIO indexIO
  )
  {
    return Iterables.transform(
        Iterables.filter(dataSegments, dataSegment -> !dataSegment.isTombstone()),
        dataSegment -> fetchSegment(dataSegment, segmentCacheManager, indexIO)
    );
  }

  // Broken out into a separate function because Some tools can't infer the
  // pair type, but if the type is given explicitly, IntelliJ inspections raises
  // an error. Creating a function keeps everyone happy.
  private static Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>> fetchSegment(
      DataSegment dataSegment,
      SegmentCacheManager segmentCacheManager,
      IndexIO indexIO
  )
  {
    return Pair.of(
        dataSegment,
        () -> {
          try {
            final Closer closer = Closer.create();
            final File file = segmentCacheManager.getSegmentFiles(dataSegment);
            closer.register(() -> segmentCacheManager.cleanup(dataSegment));
            final QueryableIndex queryableIndex = closer.register(indexIO.loadIndex(file));
            return new ResourceHolder<QueryableIndex>()
            {
              @Override
              public QueryableIndex get()
              {
                return queryableIndex;
              }

              @Override
              public void close()
              {
                try {
                  closer.close();
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            };
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
    );
  }

  /**
   * Class for fetching and analyzing existing segments in order to generate reingestion specs.
   */
  static class ExistingSegmentAnalyzer
  {
    private final Iterable<Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>>> segmentsIterable;

    private final boolean needRollup;
    private final boolean needQueryGranularity;
    private final boolean needDimensionsSpec;
    private final boolean needMetricsSpec;
    private final boolean needProjections;
    private final boolean needMultiValuedDimensions;

    // For processRollup:
    private boolean rollup = true;

    // For processQueryGranularity:
    private Granularity queryGranularity;

    // For processDimensionsSpec:
    private final BiMap<String, Integer> uniqueDims = HashBiMap.create(); // dimension name -> position in sort order
    private final Map<String, DimensionSchema> dimensionSchemaMap = new HashMap<>(); // dimension name -> schema

    // For processMetricsSpec:
    private final Set<List<AggregatorFactory>> aggregatorFactoryLists = new HashSet<>();
    private Set<String> multiValuedDimensions;
    // For processProjections
    private Map<String, AggregateProjectionSpec> projections;

    ExistingSegmentAnalyzer(
        final Iterable<Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>>> segmentsIterable,
        final boolean needRollup,
        final boolean needQueryGranularity,
        final boolean needDimensionsSpec,
        final boolean needMetricsSpec,
        final boolean needProjections,
        final boolean needMultiValuedDimensions
    )
    {
      this.segmentsIterable = segmentsIterable;
      this.needRollup = needRollup;
      this.needQueryGranularity = needQueryGranularity;
      this.needDimensionsSpec = needDimensionsSpec;
      this.needMetricsSpec = needMetricsSpec;
      this.needProjections = needProjections;
      this.needMultiValuedDimensions = needMultiValuedDimensions;
    }

    /**
     * Segments are downloaded even when just needMultiValuedDimensions=true since MSQ switches to dynamic partitioning
     * on finding any 'range' partition dimension to be multivalued at runtime, which ends up in a mismatch between
     * the compaction config and the actual segments (lastCompactionState), leading to repeated compactions.
     */
    private boolean shouldDownloadSegments()
    {

      return needRollup || needQueryGranularity || needDimensionsSpec || needMetricsSpec || needMultiValuedDimensions;
    }

    public void fetchAndProcessIfNeeded()
    {
      if (!shouldDownloadSegments()) {
        // Nothing to do; short-circuit and don't fetch segments.
        return;
      }

      multiValuedDimensions = new HashSet<>();
      final List<Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>>> segments = sortSegmentsListNewestFirst();

      for (Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>> segmentPair : segments) {
        final DataSegment dataSegment = segmentPair.lhs;

        try (final ResourceHolder<QueryableIndex> queryableIndexHolder = segmentPair.rhs.get()) {
          final QueryableIndex index = queryableIndexHolder.get();

          if (index != null) { // Avoid tombstones (null QueryableIndex)
            if (index.getMetadata() == null) {
              throw new RE(
                  "Index metadata doesn't exist for segment [%s]. Try providing explicit rollup, "
                  + "queryGranularity, dimensionsSpec, and metricsSpec.", dataSegment.getId()
              );
            }

            processRollup(index);
            processQueryGranularity(index);
            processDimensionsSpec(index);
            processMetricsSpec(index);
            processMultiValuedDimensions(index);
            processProjections(index);
          }
        }
      }
    }

    public Boolean getRollup()
    {
      if (!needRollup) {
        throw new ISE("Not computing rollup");
      }

      return rollup;
    }

    public Granularity getQueryGranularity()
    {
      if (!needQueryGranularity) {
        throw new ISE("Not computing queryGranularity");
      }

      return queryGranularity;
    }

    public DimensionsSpec getDimensionsSpec()
    {
      if (!needDimensionsSpec) {
        throw new ISE("Not computing dimensionsSpec");
      }

      final BiMap<Integer, String> orderedDims = uniqueDims.inverse();

      // Include __time as a dimension only if required, i.e., if it appears in the sort order after position 0.
      final Integer timePosition = uniqueDims.get(ColumnHolder.TIME_COLUMN_NAME);
      final boolean includeTimeAsDimension = timePosition != null && timePosition > 0;

      final List<DimensionSchema> dimensionSchemas =
          IntStream.range(0, orderedDims.size())
                   .mapToObj(i -> {
                     final String dimName = orderedDims.get(i);
                     if (ColumnHolder.TIME_COLUMN_NAME.equals(dimName) && !includeTimeAsDimension) {
                       return null;
                     } else {
                       return Preconditions.checkNotNull(
                           dimensionSchemaMap.get(dimName),
                           "Cannot find dimension[%s] from dimensionSchemaMap",
                           dimName
                       );
                     }
                   })
                   .filter(Objects::nonNull)
                   .collect(Collectors.toList());

      // Store forceSegmentSortByTime only if false, for compatibility with legacy compaction states.
      final Boolean forceSegmentSortByTime = includeTimeAsDimension ? false : null;
      return DimensionsSpec.builder()
          .setDimensions(dimensionSchemas)
          .setForceSegmentSortByTime(forceSegmentSortByTime)
          .build();
    }

    public AggregatorFactory[] getMetricsSpec()
    {
      if (!needMetricsSpec) {
        throw new ISE("Not computing metricsSpec");
      }

      if (aggregatorFactoryLists.isEmpty()) {
        return new AggregatorFactory[0];
      }

      final AggregatorFactory[] mergedAggregators = AggregatorFactory.mergeAggregators(
          aggregatorFactoryLists.stream()
                                .map(xs -> xs.toArray(new AggregatorFactory[0]))
                                .collect(Collectors.toList())
      );

      if (mergedAggregators == null) {
        throw new ISE(
            "Failed to merge existing aggregators when generating metricsSpec; "
            + "try providing explicit metricsSpec"
        );
      }

      return mergedAggregators;
    }

    @Nullable
    public List<AggregateProjectionSpec> getProjections()
    {
      if (!needProjections) {
        throw new ISE("Not computing projections");
      }
      if (projections == null || projections.isEmpty()) {
        return null;
      }
      return ImmutableList.copyOf(projections.values());
    }

    public Set<String> getMultiValuedDimensions()
    {
      return multiValuedDimensions;
    }

    /**
     * Sort {@link #segmentsIterable} in order, such that we look at later segments prior to earlier ones. Useful when
     * analyzing dimensions, as it allows us to take the latest value we see, and therefore prefer types from more
     * recent segments, if there was a change.
     *
     * Returns a List copy of the original Iterable.
     */
    private List<Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>>> sortSegmentsListNewestFirst()
    {
      final List<Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>>> segments =
          Lists.newArrayList(segmentsIterable);

      segments.sort(
          Comparator.comparing(
              o -> o.lhs.getInterval(),
              Comparators.intervalsByStartThenEnd().reversed()
          )
      );

      return segments;
    }

    private void processRollup(final QueryableIndex index)
    {
      if (!needRollup) {
        return;
      }

      // carry-overs (i.e. query granularity & rollup) are valid iff they are the same in every segment:
      // Pick rollup value if all segments being compacted have the same, non-null, value otherwise set it to false
      final Boolean isIndexRollup = index.getMetadata().isRollup();
      rollup = rollup && Boolean.valueOf(true).equals(isIndexRollup);
    }

    private void processQueryGranularity(final QueryableIndex index)
    {
      if (!needQueryGranularity) {
        return;
      }

      // Pick the finer, non-null, of the query granularities of the segments being compacted
      Granularity current = index.getMetadata().getQueryGranularity();
      queryGranularity = compareWithCurrent(queryGranularity, current);
    }

    private void processDimensionsSpec(final QueryableIndex index)
    {
      if (!needDimensionsSpec) {
        return;
      }

      final List<String> sortOrder = new ArrayList<>();

      for (final OrderBy orderBy : index.getOrdering()) {
        final String dimension = orderBy.getColumnName();
        if (orderBy.getOrder() != Order.ASCENDING) {
          throw DruidException.defensive("Order[%s] for dimension[%s] not supported", orderBy.getOrder(), dimension);
        }
        sortOrder.add(dimension);
      }

      for (String dimension : Iterables.concat(sortOrder, index.getAvailableDimensions())) {
        uniqueDims.computeIfAbsent(dimension, ignored -> uniqueDims.size());

        if (!dimensionSchemaMap.containsKey(dimension)) {
          // Possible for sortOrder to contain a dimension that doesn't exist (i.e. if it's 100% nulls).
          // In this case, omit it from dimensionSchemaMap for now. We'll skip it later if *no* segment has an existing
          // column for it.
          final ColumnHolder columnHolder = index.getColumnHolder(dimension);
          if (columnHolder != null) {
            DimensionSchema schema = columnHolder.getColumnFormat().getColumnSchema(dimension);
            // rewrite string dimensions to always use MultiValueHandling.ARRAY since it preserves the exact order of
            // the row regardless of the mode the initial ingest was using
            if (schema instanceof StringDimensionSchema) {
              schema = new StringDimensionSchema(
                  schema.getName(),
                  DimensionSchema.MultiValueHandling.ARRAY,
                  schema.hasBitmapIndex()
              );
            }
            dimensionSchemaMap.put(
                dimension,
                schema
            );
          }
        }
      }
    }

    private void processMetricsSpec(final QueryableIndex index)
    {
      if (!needMetricsSpec) {
        return;
      }

      final AggregatorFactory[] aggregators = index.getMetadata().getAggregators();
      if (aggregators != null) {
        // aggregatorFactoryLists is a Set: we don't want to store tons of copies of the same aggregator lists from
        // different segments.
        aggregatorFactoryLists.add(Arrays.asList(aggregators));
      }
    }

    private void processMultiValuedDimensions(final QueryableIndex index)
    {
      if (!needMultiValuedDimensions) {
        return;
      }
      for (String dimension : index.getAvailableDimensions()) {
        if (isMultiValuedDimension(index, dimension)) {
          multiValuedDimensions.add(dimension);
        }
      }
    }


    /**
     * gets set of projections out of {@link Metadata#getProjections()} contained in a {@link QueryableIndex}. If
     * a projection name already exists, it will be replaced with the schema of projections from this
     * {@link QueryableIndex}.
     */
    private void processProjections(final QueryableIndex index)
    {
      if (!needProjections) {
        return;
      }
      Metadata metadata = index.getMetadata();
      if (metadata != null && metadata.getProjections() != null && !metadata.getProjections().isEmpty()) {
        projections = new HashMap<>();
        for (AggregateProjectionMetadata projectionMetadata : metadata.getProjections()) {
          final AggregateProjectionMetadata.Schema schema = projectionMetadata.getSchema();
          final QueryableIndex projectionIndex = Preconditions.checkNotNull(
              index.getProjectionQueryableIndex(schema.getName())
          );
          final List<DimensionSchema> columnSchemas = Lists.newArrayListWithExpectedSize(schema.getGroupingColumns().size());
          for (String groupingColumn : schema.getGroupingColumns()) {
            if (groupingColumn.equals(schema.getTimeColumnName())) {
              columnSchemas.add(
                  projectionIndex.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME)
                                 .getColumnFormat()
                                 .getColumnSchema(groupingColumn)
              );
            } else {
              final DimensionSchema columnSchema = projectionIndex.getColumnHolder(groupingColumn)
                                                                  .getColumnFormat()
                                                                  .getColumnSchema(groupingColumn);
              // rewrite string dimensions to always use MultiValueHandling.ARRAY since it preserves the exact order of
              // the row regardless of the mode the initial ingest was using
              if (columnSchema instanceof StringDimensionSchema) {
                columnSchemas.add(
                    new StringDimensionSchema(
                        columnSchema.getName(),
                        DimensionSchema.MultiValueHandling.ARRAY,
                        columnSchema.hasBitmapIndex()
                    )
                );
              } else {
                columnSchemas.add(columnSchema);
              }
            }
          }
          projections.put(
              schema.getName(),
              new AggregateProjectionSpec(
                  schema.getName(),
                  schema.getVirtualColumns(),
                  columnSchemas,
                  schema.getAggregators()
              )
          );
        }
      }
    }

    private boolean isMultiValuedDimension(final QueryableIndex index, final String col)
    {
      ColumnCapabilities columnCapabilities = index.getColumnCapabilities(col);
      return columnCapabilities != null && columnCapabilities.hasMultipleValues().isTrue();
    }

    static Granularity compareWithCurrent(Granularity queryGranularity, Granularity current)
    {
      if (queryGranularity == null && current != null) {
        queryGranularity = current;
      } else if (queryGranularity != null
                 && current != null
                 && Granularity.IS_FINER_THAN.compare(current, queryGranularity) < 0) {
        queryGranularity = current;
      }
      // we never propagate nulls when there is at least one non-null granularity thus
      // do nothing for the case queryGranularity != null && current == null
      return queryGranularity;
    }
  }

  @VisibleForTesting
  static class SegmentProvider
  {
    private final String dataSource;
    private final CompactionInputSpec inputSpec;
    private final Interval interval;

    SegmentProvider(String dataSource, CompactionInputSpec inputSpec)
    {
      this.dataSource = Preconditions.checkNotNull(dataSource);
      this.inputSpec = inputSpec;
      this.interval = inputSpec.findInterval(dataSource);
    }

    List<DataSegment> findSegments(TaskActionClient actionClient) throws IOException
    {
      return new ArrayList<>(
          actionClient.submit(
              new RetrieveUsedSegmentsAction(dataSource, ImmutableList.of(interval))
          )
      );
    }

    void checkSegments(LockGranularity lockGranularityInUse, List<DataSegment> latestSegments)
    {
      if (latestSegments.isEmpty()) {
        throw new ISE("No segments found for compaction. Please check that datasource name and interval are correct.");
      }
      if (!inputSpec.validateSegments(lockGranularityInUse, latestSegments)) {
        throw new ISE(
            "Specified segments in the spec are different from the current used segments. "
            + "Possibly new segments would have been added or some segments have been unpublished."
        );
      }
    }
  }

  public static class Builder
  {
    private final String dataSource;
    private final SegmentCacheManagerFactory segmentCacheManagerFactory;

    private CompactionIOConfig ioConfig;
    @Nullable
    private DimensionsSpec dimensionsSpec;
    @Nullable
    private CompactionTransformSpec transformSpec;
    @Nullable
    private AggregatorFactory[] metricsSpec;
    @Nullable
    private Granularity segmentGranularity;
    @Nullable
    private ClientCompactionTaskGranularitySpec granularitySpec;
    @Nullable
    private TuningConfig tuningConfig;
    @Nullable
    private Map<String, Object> context;
    private CompactionRunner compactionRunner;
    @Nullable
    private List<AggregateProjectionSpec> projections;

    public Builder(
        String dataSource,
        SegmentCacheManagerFactory segmentCacheManagerFactory
    )
    {
      this.dataSource = dataSource;
      this.segmentCacheManagerFactory = segmentCacheManagerFactory;
    }

    public Builder interval(Interval interval)
    {
      return inputSpec(new CompactionIntervalSpec(interval, null));
    }

    public Builder segments(List<DataSegment> segments)
    {
      return inputSpec(SpecificSegmentsSpec.fromSegments(segments));
    }

    public Builder ioConfig(CompactionIOConfig ioConfig)
    {
      this.ioConfig = ioConfig;
      return this;
    }

    public Builder inputSpec(CompactionInputSpec inputSpec)
    {
      this.ioConfig = new CompactionIOConfig(inputSpec, false, null);
      return this;
    }

    public Builder inputSpec(CompactionInputSpec inputSpec, Boolean dropExisting)
    {
      this.ioConfig = new CompactionIOConfig(inputSpec, false, dropExisting);
      return this;
    }

    public Builder dimensionsSpec(DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec;
      return this;
    }

    public Builder transformSpec(CompactionTransformSpec transformSpec)
    {
      this.transformSpec = transformSpec;
      return this;
    }

    public Builder metricsSpec(AggregatorFactory[] metricsSpec)
    {
      this.metricsSpec = metricsSpec;
      return this;
    }

    public Builder segmentGranularity(Granularity segmentGranularity)
    {
      this.segmentGranularity = segmentGranularity;
      return this;
    }

    public Builder granularitySpec(ClientCompactionTaskGranularitySpec granularitySpec)
    {
      this.granularitySpec = granularitySpec;
      return this;
    }

    public Builder tuningConfig(TuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
      return this;
    }

    public Builder context(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public Builder compactionRunner(CompactionRunner compactionRunner)
    {
      this.compactionRunner = compactionRunner;
      return this;
    }

    public Builder projections(@Nullable List<AggregateProjectionSpec> projections)
    {
      this.projections = projections;
      return this;
    }

    public CompactionTask build()
    {
      return new CompactionTask(
          null,
          null,
          dataSource,
          null,
          null,
          ioConfig,
          null,
          dimensionsSpec,
          transformSpec,
          metricsSpec,
          segmentGranularity,
          granularitySpec,
          projections,
          tuningConfig,
          context,
          compactionRunner,
          segmentCacheManagerFactory
      );
    }
  }

  /**
   * Compcation Task Tuning Config.
   *
   * An extension of ParallelIndexTuningConfig. As of now, all this TuningConfig
   * does is fail if the TuningConfig contains
   * `awaitSegmentAvailabilityTimeoutMillis` that is != 0 since it is not
   * supported for Compcation Tasks.
   */
  public static class CompactionTuningConfig extends ParallelIndexTuningConfig
  {
    public static final String TYPE = "compaction";

    public static CompactionTuningConfig defaultConfig()
    {
      return new CompactionTuningConfig(
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          0L,
          null
      );
    }

    @JsonCreator
    public CompactionTuningConfig(
        @JsonProperty("targetPartitionSize") @Deprecated @Nullable Integer targetPartitionSize,
        @JsonProperty("maxRowsPerSegment") @Deprecated @Nullable Integer maxRowsPerSegment,
        @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
        @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
        @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
        @JsonProperty("skipBytesInMemoryOverheadCheck") @Nullable Boolean skipBytesInMemoryOverheadCheck,
        @JsonProperty("maxTotalRows") @Deprecated @Nullable Long maxTotalRows,
        @JsonProperty("numShards") @Deprecated @Nullable Integer numShards,
        @JsonProperty("splitHintSpec") @Nullable SplitHintSpec splitHintSpec,
        @JsonProperty("partitionsSpec") @Nullable PartitionsSpec partitionsSpec,
        @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
        @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
        @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
        @JsonProperty("forceGuaranteedRollup") @Nullable Boolean forceGuaranteedRollup,
        @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
        @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
        @JsonProperty("segmentWriteOutMediumFactory") @Nullable
        SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
        @JsonProperty("maxNumSubTasks") @Deprecated @Nullable Integer maxNumSubTasks,
        @JsonProperty("maxNumConcurrentSubTasks") @Nullable Integer maxNumConcurrentSubTasks,
        @JsonProperty("maxRetry") @Nullable Integer maxRetry,
        @JsonProperty("taskStatusCheckPeriodMs") @Nullable Long taskStatusCheckPeriodMs,
        @JsonProperty("chatHandlerTimeout") @Nullable Duration chatHandlerTimeout,
        @JsonProperty("chatHandlerNumRetries") @Nullable Integer chatHandlerNumRetries,
        @JsonProperty("maxNumSegmentsToMerge") @Nullable Integer maxNumSegmentsToMerge,
        @JsonProperty("totalNumMergeTasks") @Nullable Integer totalNumMergeTasks,
        @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
        @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
        @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
        @JsonProperty("maxColumnsToMerge") @Nullable Integer maxColumnsToMerge,
        @JsonProperty("awaitSegmentAvailabilityTimeoutMillis") @Nullable Long awaitSegmentAvailabilityTimeoutMillis,
        @JsonProperty("numPersistThreads") @Nullable Integer numPersistThreads
    )
    {
      super(
          targetPartitionSize,
          maxRowsPerSegment,
          appendableIndexSpec,
          maxRowsInMemory,
          maxBytesInMemory,
          skipBytesInMemoryOverheadCheck,
          maxTotalRows,
          numShards,
          splitHintSpec,
          partitionsSpec,
          indexSpec,
          indexSpecForIntermediatePersists,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          segmentWriteOutMediumFactory,
          maxNumSubTasks,
          maxNumConcurrentSubTasks,
          maxRetry,
          taskStatusCheckPeriodMs,
          chatHandlerTimeout,
          chatHandlerNumRetries,
          maxNumSegmentsToMerge,
          totalNumMergeTasks,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions,
          maxColumnsToMerge,
          awaitSegmentAvailabilityTimeoutMillis,
          null,
          numPersistThreads
      );

      Preconditions.checkArgument(
          awaitSegmentAvailabilityTimeoutMillis == null || awaitSegmentAvailabilityTimeoutMillis == 0,
          "awaitSegmentAvailabilityTimeoutMillis is not supported for Compcation Task"
      );
    }

    /**
     * Creates a copy of this tuning config with the partition spec changed.
     */
    public CompactionTuningConfig withPartitionsSpec(PartitionsSpec partitionsSpec)
    {
      return new CompactionTuningConfig(
          null,
          null,
          getAppendableIndexSpec(),
          getMaxRowsInMemory(),
          getMaxBytesInMemory(),
          isSkipBytesInMemoryOverheadCheck(),
          null,
          null,
          getSplitHintSpec(),
          partitionsSpec,
          getIndexSpec(),
          getIndexSpecForIntermediatePersists(),
          getMaxPendingPersists(),
          isForceGuaranteedRollup(),
          isReportParseExceptions(),
          getPushTimeout(),
          getSegmentWriteOutMediumFactory(),
          null,
          getMaxNumConcurrentSubTasks(),
          getMaxRetry(),
          getTaskStatusCheckPeriodMs(),
          getChatHandlerTimeout(),
          getChatHandlerNumRetries(),
          getMaxNumSegmentsToMerge(),
          getTotalNumMergeTasks(),
          isLogParseExceptions(),
          getMaxParseExceptions(),
          getMaxSavedParseExceptions(),
          getMaxColumnsToMerge(),
          getAwaitSegmentAvailabilityTimeoutMillis(),
          getNumPersistThreads()
      );
    }
  }
}
