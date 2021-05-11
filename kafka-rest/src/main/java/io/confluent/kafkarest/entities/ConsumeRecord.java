/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest.entities;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

@AutoValue
public abstract class ConsumeRecord {

  ConsumeRecord() {
  }

  public abstract String getClusterId();

  public abstract String getTopicName();

  @Nullable
  public abstract byte[] getKey();

  @Nullable
  public abstract byte[] getValue();

  public abstract Integer getPartition();

  public abstract Long getTimestamp();

  public abstract Long getOffset();

  public static ConsumeRecord create(
      String clusterId,
      String topicName,
      @Nullable byte[] key,
      @Nullable byte[] value,
      Integer partition,
      Long timestamp,
      Long offset) {
    return new AutoValue_ConsumeRecord(
        clusterId,
        topicName,
        key,
        value,
        partition,
        timestamp,
        offset);
  }
}