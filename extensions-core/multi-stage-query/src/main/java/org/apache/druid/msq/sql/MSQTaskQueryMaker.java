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

package org.apache.druid.msq.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.indexing.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQDestination;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.TaskReportMSQDestination;
import org.apache.druid.msq.util.MSQTaskQueryMakerUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.QueryUtils;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.Grouping;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlResults;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MSQTaskQueryMaker implements QueryMaker
{

  private static final String DESTINATION_DATASOURCE = "dataSource";
  private static final String DESTINATION_REPORT = "taskReport";

  public static final String USER_KEY = "__user";

  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.ALL;

  private final String targetDataSource;
  private final OverlordClient overlordClient;
  private final PlannerContext plannerContext;
  private final ObjectMapper jsonMapper;
  private final List<Pair<Integer, String>> fieldMapping;


  MSQTaskQueryMaker(
      @Nullable final String targetDataSource,
      final OverlordClient overlordClient,
      final PlannerContext plannerContext,
      final ObjectMapper jsonMapper,
      final List<Pair<Integer, String>> fieldMapping
  )
  {
    this.targetDataSource = targetDataSource;
    this.overlordClient = Preconditions.checkNotNull(overlordClient, "indexingServiceClient");
    this.plannerContext = Preconditions.checkNotNull(plannerContext, "plannerContext");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.fieldMapping = Preconditions.checkNotNull(fieldMapping, "fieldMapping");
  }

  @Override
  public QueryResponse<Object[]> runQuery(final DruidQuery druidQuery)
  {
    Hook.QUERY_PLAN.run(druidQuery.getQuery());
    String taskId = MSQTasks.controllerTaskId(plannerContext.getSqlQueryId());

    // SQL query context: context provided by the user, and potentially modified by handlers during planning.
    // Does not directly influence task execution, but it does form the basis for the initial native query context,
    // which *does* influence task execution.
    final QueryContext sqlQueryContext = plannerContext.queryContext();

    // Native query context: sqlQueryContext plus things that we add prior to creating a controller task.
    final Map<String, Object> nativeQueryContext = new HashMap<>(sqlQueryContext.asMap());

    // adding user
    nativeQueryContext.put(USER_KEY, plannerContext.getAuthenticationResult().getIdentity());

    final String msqMode = MultiStageQueryContext.getMSQMode(sqlQueryContext);
    if (msqMode != null) {
      MSQMode.populateDefaultQueryContext(msqMode, nativeQueryContext);
    }

    final String ctxDestination =
        DimensionHandlerUtils.convertObjectToString(MultiStageQueryContext.getDestination(sqlQueryContext));

    Object segmentGranularity;
    try {
      segmentGranularity = Optional.ofNullable(plannerContext.queryContext()
                                                             .get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY))
                                   .orElse(jsonMapper.writeValueAsString(DEFAULT_SEGMENT_GRANULARITY));
    }
    catch (JsonProcessingException e) {
      throw new IAE("Unable to deserialize the insert granularity. Please retry the query with a valid "
                    + "segment graularity");
    }

    final int maxNumTasks = MultiStageQueryContext.getMaxNumTasks(sqlQueryContext);

    if (maxNumTasks < 2) {
      throw new IAE(MultiStageQueryContext.CTX_MAX_NUM_TASKS
                    + " cannot be less than 2 since at least 1 controller and 1 worker is necessary.");
    }

    // This parameter is used internally for the number of worker tasks only, so we subtract 1
    final int maxNumWorkers = maxNumTasks - 1;
    final int rowsPerSegment = MultiStageQueryContext.getRowsPerSegment(sqlQueryContext);
    final int maxRowsInMemory = MultiStageQueryContext.getRowsInMemory(sqlQueryContext);
    final IndexSpec indexSpec = MultiStageQueryContext.getIndexSpec(sqlQueryContext, jsonMapper);
    final boolean finalizeAggregations = MultiStageQueryContext.isFinalizeAggregations(sqlQueryContext);

    final List<Interval> replaceTimeChunks =
        Optional.ofNullable(sqlQueryContext.get(DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS))
                .map(
                    s -> {
                      if (s instanceof String && "all".equals(StringUtils.toLowerCase((String) s))) {
                        return Intervals.ONLY_ETERNITY;
                      } else {
                        final String[] parts = ((String) s).split("\\s*,\\s*");
                        final List<Interval> intervals = new ArrayList<>();

                        for (final String part : parts) {
                          intervals.add(Intervals.of(part));
                        }

                        return intervals;
                      }
                    }
                )
                .orElse(null);

    // For assistance computing return types if !finalizeAggregations.
    final Map<String, ColumnType> aggregationIntermediateTypeMap =
        finalizeAggregations ? null /* Not needed */ : buildAggregationIntermediateTypeMap(druidQuery);

    final List<SqlTypeName> sqlTypeNames = new ArrayList<>();
    final List<ColumnType> columnTypeList = new ArrayList<>();
    final List<ColumnMapping> columnMappings = QueryUtils.buildColumnMappings(fieldMapping, druidQuery);

    for (final Pair<Integer, String> entry : fieldMapping) {
      final String queryColumn = druidQuery.getOutputRowSignature().getColumnName(entry.getKey());

      final SqlTypeName sqlTypeName;

      if (!finalizeAggregations && aggregationIntermediateTypeMap.containsKey(queryColumn)) {
        final ColumnType druidType = aggregationIntermediateTypeMap.get(queryColumn);
        sqlTypeName = new RowSignatures.ComplexSqlType(SqlTypeName.OTHER, druidType, true).getSqlTypeName();
      } else {
        sqlTypeName = druidQuery.getOutputRowType().getFieldList().get(entry.getKey()).getType().getSqlTypeName();
      }
      sqlTypeNames.add(sqlTypeName);
      columnTypeList.add(druidQuery.getOutputRowSignature().getColumnType(queryColumn).orElse(ColumnType.STRING));
    }

    final MSQDestination destination;

    if (targetDataSource != null) {
      if (ctxDestination != null && !DESTINATION_DATASOURCE.equals(ctxDestination)) {
        throw new IAE("Cannot INSERT with destination [%s]", ctxDestination);
      }

      Granularity segmentGranularityObject;
      try {
        segmentGranularityObject = jsonMapper.readValue((String) segmentGranularity, Granularity.class);
      }
      catch (Exception e) {
        throw new ISE("Unable to convert %s to a segment granularity", segmentGranularity);
      }

      final List<String> segmentSortOrder = MultiStageQueryContext.getSortOrder(sqlQueryContext);

      MSQTaskQueryMakerUtils.validateSegmentSortOrder(
          segmentSortOrder,
          fieldMapping.stream().map(f -> f.right).collect(Collectors.toList())
      );

      destination = new DataSourceMSQDestination(
          targetDataSource,
          segmentGranularityObject,
          segmentSortOrder,
          replaceTimeChunks
      );
    } else {
      if (ctxDestination != null && !DESTINATION_REPORT.equals(ctxDestination)) {
        throw new IAE("Cannot SELECT with destination [%s]", ctxDestination);
      }

      destination = TaskReportMSQDestination.instance();
    }

    final Map<String, Object> nativeQueryContextOverrides = new HashMap<>();

    // Add appropriate finalization to native query context.
    nativeQueryContextOverrides.put(QueryContexts.FINALIZE_KEY, finalizeAggregations);

    final MSQSpec querySpec =
        MSQSpec.builder()
               .query(druidQuery.getQuery().withOverriddenContext(nativeQueryContextOverrides))
               .columnMappings(new ColumnMappings(columnMappings))
               .destination(destination)
               .assignmentStrategy(MultiStageQueryContext.getAssignmentStrategy(sqlQueryContext))
               .tuningConfig(new MSQTuningConfig(maxNumWorkers, maxRowsInMemory, rowsPerSegment, indexSpec))
               .build();

    final MSQControllerTask controllerTask = new MSQControllerTask(
        taskId,
        querySpec.withOverriddenContext(nativeQueryContext),
        MSQTaskQueryMakerUtils.maskSensitiveJsonKeys(plannerContext.getSql()),
        plannerContext.queryContextMap(),
        SqlResults.Context.fromPlannerContext(plannerContext),
        sqlTypeNames,
        columnTypeList,
        null
    );

    FutureUtils.getUnchecked(overlordClient.runTask(taskId, controllerTask), true);
    return QueryResponse.withEmptyContext(Sequences.simple(Collections.singletonList(new Object[]{taskId})));
  }

  private static Map<String, ColumnType> buildAggregationIntermediateTypeMap(final DruidQuery druidQuery)
  {
    final Grouping grouping = druidQuery.getGrouping();

    if (grouping == null) {
      return Collections.emptyMap();
    }

    final Map<String, ColumnType> retVal = new HashMap<>();

    for (final AggregatorFactory aggregatorFactory : grouping.getAggregatorFactories()) {
      retVal.put(aggregatorFactory.getName(), aggregatorFactory.getIntermediateType());
    }

    return retVal;
  }

}
