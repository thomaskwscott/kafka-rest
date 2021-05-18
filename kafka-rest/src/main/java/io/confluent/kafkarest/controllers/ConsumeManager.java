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
import io.confluent.kafkarest.entities.TopicPartitionRecords;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A service to fetch records from Kafka.
 */
public interface ConsumeManager {

  /**
   * Returns the list of Kafka {@link TopicPartitionRecords records} belonging to the {@link
   * io.confluent.kafkarest.entities.Topic} {@link io.confluent.kafkarest.entities.Partition}
   * with the given {@code offset} or {@code timestamp} and {@code pageSize}.
   */
  CompletableFuture<Map<TopicPartitionRecords,List<ConsumeRecord>>> getRecords(
      String clusterId,
      String topicName,
      Integer partitionId,
      Optional<Long> offset,
      Optional<Long> timestamp,
      Integer pageSize);

  /**
   * Returns the list of Kafka {@link TopicPartitionRecords records} belonging to the {@link
   * io.confluent.kafkarest.entities.Topic} with the given {@code offsets} or {@code timestamp}
   * and {@code pageSize}.
   */
  CompletableFuture<Map<TopicPartitionRecords,List<ConsumeRecord>>> getRecords(
      String clusterId,
      String topicName,
      Optional<Map<Integer,Long>> offsets,
      Optional<Long> timestamp,
      Integer pageSize);

}
