/*
 * Copyright 2021 Confluent Inc.
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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.confluent.kafkarest.controllers.Entities.checkEntityExists;
import static java.util.Objects.requireNonNull;

public class ConsumeManagerImpl implements ConsumeManager {

  private static final int POLL_MS = 2000;

  private final Consumer<byte[], byte[]> consumer;
  private final ClusterManager clusterManager;

  @Inject
  ConsumeManagerImpl(
      Consumer consumer,
      ClusterManager clusterManager) {
    super();
    this.consumer = requireNonNull(consumer);
    this.clusterManager = requireNonNull(clusterManager);
  }

  @Override
  public CompletableFuture<List<ConsumeRecord<byte[], byte[]>>> getRecords(
      String clusterId,
      String topicName,
      Integer partitionId,
      Long offset,
      Integer pageSize) {
    return clusterManager.getCluster(clusterId)
        .thenApply(
            cluster -> checkEntityExists(cluster, "Cluster %s could not be found.", clusterId))
        .thenApply(
            cluster -> toConsumeRecords(
                clusterId,
                fetchPage(
                    new TopicPartition(topicName, partitionId),
                    offset
                ),
                pageSize
            )
        );
  }

  private List<ConsumerRecord<byte[],byte[]>> fetchPage(
      TopicPartition tp,
      Long offset) {
    consumer.assign(Collections.singletonList(tp));
    consumer.seek(tp, offset);
    return consumer.poll(Duration.ofMillis(POLL_MS)).records(tp);
  }

  private List<ConsumeRecord<byte[],byte[]>> toConsumeRecords(
      String clusterId, List<ConsumerRecord<byte[],byte[]>> records, Integer pageSize) {
    if (records.size() > pageSize) records = records.subList(0,pageSize);
    return records.stream().map(
        record -> ConsumeRecord.create(
            clusterId,
            record.topic(),
            record.key(),
            record.value(),
            record.partition(),
            record.timestamp(),
            record.offset())
    ).collect(Collectors.toList());
  }



}
