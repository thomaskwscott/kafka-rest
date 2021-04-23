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

package io.confluent.kafkarest.resources.v3;

import io.confluent.kafkarest.controllers.ConsumeManager;
import io.confluent.kafkarest.entities.ConsumeRecord;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.kafkarest.entities.v3.*;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.sse.OutboundSseEvent;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseEventSink;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions/{partitionId}/consume")
@ResourceName("api.v3.consume.*")
public final class StreamingConsumeAction {

  private final Provider<ConsumeManager> consumeManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  private Sse sse;
  private OutboundSseEvent.Builder eventBuilder;

  @Inject
  public StreamingConsumeAction(
      Provider<ConsumeManager> consumeManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory
  ) {
    this.consumeManager = requireNonNull(consumeManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @GET
  @Produces(MediaType.SERVER_SENT_EVENTS)
  @PerformanceMetric("v3.consume.list")
  @ResourceName("api.v3.consume.list")
  public void streamConsumeRecords(
      @Context SseEventSink sseEventSink,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @PathParam("partitionId") Integer partitionId,
      @QueryParam("offset") Long offset
  ) {
    Long nextOffset = 0l;
    try {
      while (true) {
        List<ConsumeRecord<byte[], byte[]>> fetched = consumeManager.get()
            .getRecords(clusterId, topicName, partitionId, nextOffset, 1).get();

        fetched.stream().forEach(message ->
            sseEventSink.send(this.eventBuilder
                .id(getResourceName(message))
                .name("KafkaConsumeRecord")
                .mediaType(MediaType.APPLICATION_JSON_TYPE)
                .data(ConsumeRecord.class, toConsumeRecordData(message))
                .reconnectDelay(1000)
                .comment("this is offset " + message.getOffset())
                .build())
        );

        if (!fetched.isEmpty()) {
          nextOffset = fetched.stream().max(Comparator.
              comparing(ConsumeRecord::getOffset)).get().getOffset() + 1;
        }
      }
    } catch (Exception e) {
      //e.printStackTrace();
    } finally {
      sseEventSink.close();
    }

  }


  @Context
  public void setSse(Sse sse) {
    this.sse = sse;
    this.eventBuilder = sse.newEventBuilder();
  }



  private ConsumeRecordData toConsumeRecordData(ConsumeRecord<byte[], byte[]> record) {
    return ConsumeRecordData.fromConsumeRecord(record)
        .setMetadata(
            Resource.Metadata.builder()
                .setSelf(
                    urlFactory.create(
                        "v3",
                        "clusters",
                        record.getClusterId(),
                        "topic_name",
                        record.getTopicName(),
                        "partition_id",
                        Integer.toString(record.getPartition()),
                        "offset",
                        Long.toString(record.getOffset())))
                .setResourceName(
                    getResourceName(record))
                .build())
        .build();
  }

  private String getResourceName(ConsumeRecord<byte[], byte[]> record) {
    return crnFactory.create(
        "kafka",
        record.getClusterId(),
        "topic_name",
        record.getTopicName(),
        "partition_id",
        Integer.toString(record.getPartition()),
        "offset",
        Long.toString(record.getOffset()));
  }
}
