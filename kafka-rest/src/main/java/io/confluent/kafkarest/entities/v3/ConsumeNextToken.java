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

import java.util.Map;

@AutoValue
public abstract class ConsumeNextToken {

  ConsumeNextToken() {
  }

  @JsonProperty("cluster_id")
  public abstract String getClusterId();

  @JsonProperty("topic_name")
  public abstract String getTopicName();

  @JsonProperty("timestamp")
  public abstract long getTimestamp();

  @JsonProperty("page_size")
  public abstract int getPageSize();

  @JsonProperty("position")
  public abstract Map<Integer,Long> getPosition();

  public static Builder builder() {
    return new AutoValue_ConsumeNextToken.Builder();
  }

  @JsonCreator
  static ConsumeNextToken fromJson(
      @JsonProperty("cluster_id") String clusterId,
      @JsonProperty("topic_name") String topicName,
      @JsonProperty("timestamp") Long timestamp,
      @JsonProperty("page_size") Integer pageSize,
      @JsonProperty("position") Map<Integer,Long> position
  ) {
    return builder()
        .setClusterId(clusterId)
        .setTopicName(topicName)
        .setTimestamp(timestamp)
        .setPageSize(pageSize)
        .setPosition(position)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    Builder() {
    }

    public abstract Builder setClusterId(String clusterId);

    public abstract Builder setTopicName(String topicName);

    public abstract Builder setTimestamp(Long timestamp);

    public abstract Builder setPageSize(Integer pageSize);

    public abstract Builder setPosition(Map<Integer, Long> position);

    public abstract ConsumeNextToken build();
  }
}

