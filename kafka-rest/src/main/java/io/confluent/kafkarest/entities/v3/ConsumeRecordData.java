/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.entities.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import io.confluent.kafkarest.entities.ConsumeRecord;

import java.util.Optional;

@AutoValue
public abstract class ConsumeRecordData extends Resource {

  ConsumeRecordData() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("partition_id")
  public abstract int getPartitionId();

  @JsonProperty("key")
  public abstract String getKey();

  @JsonProperty("value")
  public abstract Optional<String> getValue();

  @JsonProperty("timestamp")
  public abstract Long getTimestamp();

  @JsonProperty("offset")
  public abstract Long getOffset();

  public static Builder builder() {
    return new AutoValue_ConsumeRecordData.Builder().setKind("KafkaConsumeRecord");
  }

  public static Builder fromConsumeRecord(ConsumeRecord consumeRecord) {
    return builder()
        .setClusterId(consumeRecord.getClusterId())
        .setTopicName(consumeRecord.getTopicName())
        .setPartitionId(consumeRecord.getPartition())
        .setKey(consumeRecord.getKey().toString()) // <- fix this to deserialize properly
        .setValue(consumeRecord.getValue().toString()) // <- fix this to deserialize properly
        .setOffset(consumeRecord.getOffset())
        .setTimestamp(consumeRecord.getTimestamp());
  }

  // CHECKSTYLE:OFF:ParameterNumber
  @JsonCreator
  static ConsumeRecordData fromJson(
      @JsonProperty("kind") String kind,
      @JsonProperty("metadata") Metadata metadata,
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("partition_id") Integer partitionId,
      @JsonProperty("key") String key,
      @JsonProperty("value") String value,
      @JsonProperty("timestamp") Long timestamp,
      @JsonProperty("offset") Long offset
  ) {
    return builder()
        .setKind(kind)
        .setMetadata(metadata)
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setPartitionId(partitionId)
        .setKey(key)
        .setValue(value)
        .setTimestamp(timestamp)
        .setOffset(offset)
        .build();
  }
  // CHECKSTYLE:ON:ParameterNumber

  @AutoValue.Builder
  public abstract static class Builder extends Resource.Builder<Builder> {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setPartitionId(int partitionId);

    public abstract Builder setKey(String key);

    public abstract Builder setValue(String value);

    public abstract Builder setTimestamp(Long timestamp);

    public abstract Builder setOffset(Long offset);

    public abstract ConsumeRecordData build();
  }
}

