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
import io.confluent.kafkarest.entities.TopicPartitionRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.kafkarest.controllers.Entities.checkEntityExists;
import static java.util.Objects.requireNonNull;

public class ConsumeManagerImpl implements ConsumeManager {

  private static final int POLL_MS = 2000;

  private final Consumer<byte[], byte[]> consumer;
  private final ClusterManager clusterManager;
  private final TopicManager topicManager;

  @Inject
  ConsumeManagerImpl(
      Consumer consumer,
      ClusterManager clusterManager,
      TopicManager topicManager) {
    super();
    this.consumer = requireNonNull(consumer);
    this.clusterManager = requireNonNull(clusterManager);
    this.topicManager = requireNonNull(topicManager);
  }

  @Override
  public CompletableFuture<Map<TopicPartitionRecords, List<ConsumeRecord>>> getRecords(
      String clusterId,
      String topicName,
      Integer partitionId,
      Optional<Long> offset,
      Optional<Long> timestamp,
      Integer pageSize) {
    return clusterManager.getCluster(clusterId)
        .thenApply(
            cluster -> checkEntityExists(cluster, "Cluster %s could not be found.", clusterId))
        .thenApply(
            cluster -> {
              TopicPartition fetchPartition = new TopicPartition(topicName, partitionId);
              long fetchOffset = 0L;
              if (offset.isPresent()) {
                fetchOffset = offset.get();
              } else {
                if (timestamp.isPresent()) {
                  fetchOffset = consumer.offsetsForTimes(
                      Collections.singletonMap(fetchPartition, timestamp.get()))
                      .get(fetchPartition).offset();
                } else {
                  fetchOffset = consumer.endOffsets(
                      Collections.singletonList(fetchPartition))
                      .get(fetchPartition);
                }
              }

              Map<TopicPartition,Long> assignments = Collections.singletonMap(fetchPartition,
                  fetchOffset);
              return toTopicPartitionRecords(
                  clusterId,
                  fetchPage(assignments),
                  assignments,
                  pageSize
              );
            }
        );
  }

  @Override
  public CompletableFuture<Map<TopicPartitionRecords, List<ConsumeRecord>>> getRecords(
      String clusterId,
      String topicName,
      Optional<Map<Integer, Long>> offsets,
      Optional<Long> timestamp,
      Integer pageSize) {
    return clusterManager.getCluster(clusterId)
        .thenApply(
            cluster -> checkEntityExists(cluster, "Cluster %s could not be found.", clusterId))
        .thenCompose(
            cluster ->
              topicManager.getTopic(clusterId, topicName))
                  .thenApply(
                      topic -> {

                        // if we are provided offsets we use those
                        final Map<TopicPartition, Long> offsetAssignments = offsets.orElse(
                            new HashMap<>()).entrySet().stream().map(
                              entry -> new AbstractMap.SimpleEntry<TopicPartition, Long>(
                                  new TopicPartition(topicName, entry.getKey()), entry.getValue())
                          ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                        // we need to fill in any missing partitions with the timestamp offset
                        // this will only occur on the first fetch
                        List<TopicPartition> partitions = topic.orElseThrow(
                            IllegalArgumentException::new)
                            .getPartitions().stream().map(
                              partition -> new TopicPartition(topicName, partition.getPartitionId())
                          ).collect(Collectors.toList());

                        final Map<TopicPartition, Long> timestampAssignments = consumer
                            .offsetsForTimes(
                              partitions.stream().filter(partition -> !offsetAssignments.keySet()
                                  .contains(partition)).map(
                                    topicPartition -> new AbstractMap
                                        .SimpleEntry<TopicPartition, Long>(
                                      topicPartition, timestamp.orElse(System.currentTimeMillis()))
                              ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                            .entrySet().stream()
                              .collect(Collectors.toMap(
                                  Map.Entry::getKey,
                                  e -> e.getValue().offset()));

                        Map<TopicPartition,Long> assignments = Stream.of(offsetAssignments,
                            timestampAssignments)
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue));
                        return toTopicPartitionRecords(
                            clusterId,
                            fetchPage(assignments),
                            assignments,
                            pageSize
                        );
                      }
              );
  }

  private Map<TopicPartition,List<ConsumerRecord>> fetchPage(
      Map<TopicPartition,Long> assignments) {
    consumer.assign(assignments.keySet());
    assignments.entrySet().stream().forEach(
        assignment -> consumer.seek(assignment.getKey(),assignment.getValue())
    );
    ConsumerRecords records = consumer.poll(Duration.ofMillis(POLL_MS));
    return assignments.keySet().stream().map(
        topicPartition -> new AbstractMap.SimpleEntry<TopicPartition,List<ConsumerRecord>>(
            topicPartition,records.records(topicPartition))
    ).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
  }

  private Map<TopicPartitionRecords,List<ConsumeRecord>> toTopicPartitionRecords(
      String clusterId,
      Map<TopicPartition,List<ConsumerRecord>> records,
      Map<TopicPartition,Long> assignments,
      Integer pageSize) {

    // check if we need to trim
    int totalRecordCount = records.values().stream()
        .mapToInt(List::size)
        .sum();
    if (totalRecordCount > pageSize) {
      // flatten, sort by timestamp, truncate top page size and re-separate
      records = records.values().stream()
          .flatMap(List::stream)
          .sorted(Comparator.comparing(ConsumerRecord::timestamp))
          .limit(pageSize)
          .sorted(Comparator.comparing(ConsumerRecord::offset))
          .collect(Collectors.groupingBy(record -> new TopicPartition(record.topic(),
              record.partition())));
    }

    Map<TopicPartition, List<ConsumerRecord>> finalRecords1 = records;
    return assignments.entrySet().stream().map(
        e -> new AbstractMap.SimpleEntry<TopicPartitionRecords,List<ConsumeRecord>>(
        TopicPartitionRecords.create(
            clusterId,
            e.getKey().topic(),
            e.getKey().partition(),
            e.getValue(),
            finalRecords1.get(e.getKey()).stream().max(Comparator.comparing(r -> r.offset()))
                .get().offset()
        ), finalRecords1.get(e.getKey()).stream().map(r -> ConsumeRecord.create(clusterId,
            e.getKey().topic(),
            (byte[])r.key(),
            (byte[])r.value(),
            e.getKey().partition(),
            r.timestamp(),
            r.offset()
        )).collect(Collectors.toList()))
    ).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
  }
}
