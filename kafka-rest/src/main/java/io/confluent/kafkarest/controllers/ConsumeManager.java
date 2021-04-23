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

package io.confluent.kafkarest.controllers;

import io.confluent.kafkarest.entities.ConsumeRecord;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A service to fetch records from Kafka.
 */
public interface ConsumeManager {

  /**
   * Returns the list of Kafka {@link ConsumeRecord records} belonging to the {@link
   * io.confluent.kafkarest.entities.Topic} {@link io.confluent.kafkarest.entities.Partition}
   * with the given {@code offset} and {@code pageSize}.
   */
  CompletableFuture<List<ConsumeRecord<byte[],byte[]>>> getRecords(
      String clusterId,
      String topicName,
      Integer partitionId,
      Long offset,
      Integer pageSize);

}
