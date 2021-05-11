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
import io.confluent.kafkarest.entities.EmbeddedFormat;
import io.confluent.kafkarest.entities.v3.ConsumeRecordData;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.sse.OutboundSseEvent;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseEventSink;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/partitions/{partitionId}/stream")
@ResourceName("api.v3.consume.*")
public final class StreamingConsumeByPartitionAction {

  private final Provider<ConsumeManager> consumeManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  private Sse sse;
  private OutboundSseEvent.Builder eventBuilder;

  @Inject
  public StreamingConsumeByPartitionAction(
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
      @DefaultValue("BINARY") @PathParam("format") String format,
      @DefaultValue("-1") @QueryParam("offset") Long offset
  ) {
    Long nextOffset = 0L;
    try {
      while (!sseEventSink.isClosed()) {
        List<ConsumeRecord> fetched = consumeManager.get()
            .getRecords(clusterId,
                topicName,
                partitionId,
                Optional.of(nextOffset),
                Optional.empty(),
                1).get();

        fetched.stream().forEach(message ->
            sseEventSink.send(this.eventBuilder
                .id(getResourceName(message))
                .name("KafkaConsumeRecord")
                .mediaType(MediaType.APPLICATION_JSON_TYPE)
                .data(ConsumeRecord.class, toConsumeRecordData(message,
                    EmbeddedFormat.valueOf(format)))
                .reconnectDelay(1000)
                .comment("this is offset " + message.getOffset())
                .build())
        );

        if (!fetched.isEmpty()) {
          nextOffset = fetched.stream().max(
              Comparator.comparing(ConsumeRecord::getOffset)).get()
              .getOffset() + 1;
        }

        System.out.println("sseEventSink closed: " + sseEventSink.isClosed());

      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      sseEventSink.close();
    }

  }


  @Context
  public void setSse(Sse sse) {
    this.sse = sse;
    this.eventBuilder = sse.newEventBuilder();
  }



  private ConsumeRecordData toConsumeRecordData(ConsumeRecord record, EmbeddedFormat format) {
    return ConsumeRecordData.fromConsumeRecord(record, format)
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

  private String getResourceName(ConsumeRecord record) {
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
