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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.extension.KafkaConfigOverrides;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;

public class KafkaIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<KafkaTopicPartition, Long>
{
  private final Map<String, Object> consumerProperties;
  private final long pollTimeout;
  private final KafkaConfigOverrides configOverrides;

  private final boolean multiTopic;

  @JsonCreator
  public KafkaIndexTaskIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null for backward compabitility
      @JsonProperty("baseSequenceName") String baseSequenceName,
      // startPartitions and endPartitions exist to be able to read old ioConfigs in metadata store
      @JsonProperty("startPartitions") @Nullable
      @Deprecated SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> startPartitions,
      @JsonProperty("endPartitions") @Nullable
      @Deprecated SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> endPartitions,
      // startSequenceNumbers and endSequenceNumbers must be set for new versions
      @JsonProperty("startSequenceNumbers")
      @Nullable SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> startSequenceNumbers,
      @JsonProperty("endSequenceNumbers")
      @Nullable SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> endSequenceNumbers,
      @JsonProperty("consumerProperties") Map<String, Object> consumerProperties,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat,
      @JsonProperty("configOverrides") @Nullable KafkaConfigOverrides configOverrides,
      @JsonProperty("multiTopic") @Nullable Boolean multiTopic,
      @JsonProperty("refreshRejectionPeriodsInMinutes") Long refreshRejectionPeriodsInMinutes
  )
  {
    super(
        taskGroupId,
        baseSequenceName,
        startSequenceNumbers == null
        ? Preconditions.checkNotNull(startPartitions, "startPartitions").asStartPartitions(true)
        : startSequenceNumbers,
        endSequenceNumbers == null ? endPartitions : endSequenceNumbers,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat,
        refreshRejectionPeriodsInMinutes
    );

    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    this.pollTimeout = pollTimeout != null ? pollTimeout : KafkaSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;
    this.configOverrides = configOverrides;
    this.multiTopic = multiTopic != null ? multiTopic : KafkaSupervisorIOConfig.DEFAULT_IS_MULTI_TOPIC;

    final SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> myEndSequenceNumbers = getEndSequenceNumbers();
    for (KafkaTopicPartition partition : myEndSequenceNumbers.getPartitionSequenceNumberMap().keySet()) {
      Preconditions.checkArgument(
          myEndSequenceNumbers.getPartitionSequenceNumberMap()
                              .get(partition)
                              .compareTo(getStartSequenceNumbers().getPartitionSequenceNumberMap().get(partition)) >= 0,
          "end offset must be >= start offset for partition[%s]",
          partition
      );
    }
  }

  public KafkaIndexTaskIOConfig(
      int taskGroupId,
      String baseSequenceName,
      SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> startSequenceNumbers,
      SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> endSequenceNumbers,
      Map<String, Object> consumerProperties,
      Long pollTimeout,
      Boolean useTransaction,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      InputFormat inputFormat,
      KafkaConfigOverrides configOverrides,
      Long refreshRejectionPeriodsInMinutes
  )
  {
    this(
        taskGroupId,
        baseSequenceName,
        null,
        null,
        startSequenceNumbers,
        endSequenceNumbers,
        consumerProperties,
        pollTimeout,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat,
        configOverrides,
        KafkaSupervisorIOConfig.DEFAULT_IS_MULTI_TOPIC,
        refreshRejectionPeriodsInMinutes
    );
  }

  /**
   * This method is for compatibilty so that newer version of KafkaIndexTaskIOConfig can be read by
   * old version of Druid. Note that this method returns end sequence numbers instead of start. This is because
   * {@link SeekableStreamStartSequenceNumbers} didn't exist before.
   */
  @JsonProperty
  @Deprecated
  public SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> getStartPartitions()
  {
    // Converting to start sequence numbers. This is allowed for Kafka because the start offset is always inclusive.
    final SeekableStreamStartSequenceNumbers<KafkaTopicPartition, Long> startSequenceNumbers = getStartSequenceNumbers();
    return new SeekableStreamEndSequenceNumbers<>(
        startSequenceNumbers.getStream(),
        startSequenceNumbers.getPartitionSequenceNumberMap()
    );
  }

  /**
   * This method is for compatibilty so that newer version of KafkaIndexTaskIOConfig can be read by
   * old version of Druid.
   */
  @JsonProperty
  @Deprecated
  public SeekableStreamEndSequenceNumbers<KafkaTopicPartition, Long> getEndPartitions()
  {
    return getEndSequenceNumbers();
  }

  @JsonProperty
  public Map<String, Object> getConsumerProperties()
  {
    return consumerProperties;
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @JsonProperty
  public KafkaConfigOverrides getConfigOverrides()
  {
    return configOverrides;
  }

  @JsonProperty
  public boolean isMultiTopic()
  {
    return multiTopic;
  }

  @Override
  public String toString()
  {
    return "KafkaIndexTaskIOConfig{" +
           "taskGroupId=" + getTaskGroupId() +
           ", baseSequenceName='" + getBaseSequenceName() + '\'' +
           ", startSequenceNumbers=" + getStartSequenceNumbers() +
           ", endSequenceNumbers=" + getEndSequenceNumbers() +
           ", consumerProperties=" + consumerProperties +
           ", pollTimeout=" + pollTimeout +
           ", useTransaction=" + isUseTransaction() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           ", configOverrides=" + getConfigOverrides() +
           '}';
  }
}
