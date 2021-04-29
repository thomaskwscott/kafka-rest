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
import io.confluent.kafkarest.entities.Topic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import java.time.Duration;
import java.util.*;
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
  public CompletableFuture<List<ConsumeRecord<byte[], byte[]>>> getRecords(
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
              long fetchOffset = 0l;
              if (timestamp.isPresent()) {
                fetchOffset = consumer.offsetsForTimes(
                    Collections.singletonMap(fetchPartition, timestamp.get()))
                    .get(fetchPartition).offset();
              } else if (!offset.isPresent()) {
                // we have to be sure that latest offset - pagesize is not before the earliest offset in th log.
                // If it is the auto.offset.reset policy will kick in.
                // TODO: write a test to verify this
                long firstOffset = consumer.beginningOffsets(Collections.singletonList(fetchPartition)).get(fetchPartition);
                long pageStartOffset = consumer.endOffsets(Collections.singletonList(fetchPartition)).get(fetchPartition) - pageSize;
                fetchOffset = Math.max(firstOffset,pageStartOffset);
              } else {
                fetchOffset = offset.get();
              }

              return toConsumeRecords(
                  clusterId,
                  fetchPage(
                      Collections.singletonMap(fetchPartition,fetchOffset)
                  ),
                  pageSize
              );
            }
        );
  }

  @Override
  public CompletableFuture<List<ConsumeRecord<byte[], byte[]>>> getRecords(
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
                        final Map<TopicPartition, Long> offsetAssignments = offsets.orElse(new HashMap<>()).entrySet().stream().map(
                              entry -> new AbstractMap.SimpleEntry<TopicPartition, Long>(
                                  new TopicPartition(topicName, entry.getKey()), entry.getValue())
                          ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                        // we need to fill in any missing partitions with the timestamp offset
                        List<TopicPartition> partitions = topic.orElseThrow(IllegalArgumentException::new).getPartitions().stream().map(
                              partition -> new TopicPartition(topicName, partition.getPartitionId())
                          ).collect(Collectors.toList());

                        if (partitions.size() > offsetAssignments.size())
                          if (!timestamp.isPresent()) throw new IllegalArgumentException(
                              "Some partitions do not have provided offsets and a timestamp is not provided as an alternative");

                        final Map<TopicPartition,Long> timestampAssignments = consumer.offsetsForTimes(
                          partitions.stream().filter(partition -> !offsetAssignments.keySet().contains(partition)).map(
                              topicPartition -> new AbstractMap.SimpleEntry<TopicPartition, Long>(
                                  topicPartition, timestamp.get())
                          ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))).entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

                        return toConsumeRecords(
                            clusterId,
                            fetchPage(
                                Stream.of(offsetAssignments,timestampAssignments)
                                    .flatMap(map -> map.entrySet().stream())
                                    .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue))
                            ),
                            pageSize
                        );
                      }
              );
  }

  private Map<TopicPartition,List<ConsumerRecord<byte[],byte[]>>> fetchPage(
      Map<TopicPartition,Long> assignments) {
    consumer.assign(assignments.keySet());
    assignments.entrySet().stream().forEach(
        assignment -> consumer.seek(assignment.getKey(),assignment.getValue())
    );
    ConsumerRecords records = consumer.poll(Duration.ofMillis(POLL_MS));
    return assignments.keySet().stream().map(
        topicPartition -> new AbstractMap.SimpleEntry<TopicPartition,List<ConsumerRecord<byte[],byte[]>>>(
            topicPartition,records.records(topicPartition))
    ).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
  }

  private List<ConsumeRecord<byte[],byte[]>> toConsumeRecords(
      String clusterId, Map<TopicPartition,List<ConsumerRecord<byte[],byte[]>>> records, Integer pageSize) {
    int recordsPerPartition = pageSize / records.keySet().size();
    // pick any partition and return 1 record
    if (recordsPerPartition == 0) {
      Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> finalRecords = records;
      records = records.entrySet().stream()
          .filter(entry -> entry.getKey()== finalRecords.keySet()
              .toArray()[new Random().nextInt(finalRecords.keySet().size())])
          .collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue));
      recordsPerPartition = 1;
    }

    int finalRecordsPerPartition = recordsPerPartition;
    return records.entrySet().stream().flatMap(
        entry -> entry.getValue().subList(0,
            Math.min(finalRecordsPerPartition,entry.getValue().size()))
            .stream().map(
              record -> ConsumeRecord.create(
                clusterId,
                record.topic(),
                record.key(),
                record.value(),
                record.partition(),
                record.timestamp(),
                record.offset())
        )
    ).collect(Collectors.toList());
  }



}
