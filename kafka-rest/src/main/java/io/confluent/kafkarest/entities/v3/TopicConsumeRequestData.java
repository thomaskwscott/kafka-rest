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
import com.google.common.collect.ImmutableList;

import java.util.List;

@AutoValue
public abstract class TopicConsumeRequestData {

  TopicConsumeRequestData() {
  }

  @JsonProperty("data")
  public abstract ImmutableList<PartitionOffsetEntry> getData();

  public static TopicConsumeRequestData create(List<PartitionOffsetEntry> data) {
    return new AutoValue_TopicConsumeRequestData(ImmutableList.copyOf(data));
  }

  @JsonCreator
  static TopicConsumeRequestData fromJson(@JsonProperty("data") List<PartitionOffsetEntry> data) {
    return create(data);
  }

  @AutoValue
  public abstract static class PartitionOffsetEntry {

    @JsonProperty("partition_id")
    public abstract Integer getPartitionId();

    @JsonProperty("offset")
    public abstract Long getOffset();

    public static Builder builder() {
      return new AutoValue_TopicConsumeRequestData_PartitionOffsetEntry.Builder();
    }

    @JsonCreator
    static PartitionOffsetEntry fromJson(
        @JsonProperty("partition_id") Integer partitionId,
        @JsonProperty("offset") Long offset
    ) {
      return builder()
          .setPartitionId(partitionId)
          .setOffset(offset)
          .build();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setPartitionId(Integer partitionId);

      public abstract Builder setOffset(Long offset);

      public abstract PartitionOffsetEntry build();
    }
  }
}
