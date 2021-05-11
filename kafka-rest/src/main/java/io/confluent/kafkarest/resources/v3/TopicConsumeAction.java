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
import io.confluent.kafkarest.entities.v3.ConsumeRecordDataList;
import io.confluent.kafkarest.entities.v3.ListConsumeRecordsResponse;
import io.confluent.kafkarest.entities.v3.Resource;
import io.confluent.kafkarest.entities.v3.ResourceCollection;
import io.confluent.kafkarest.entities.v3.TopicConsumeRequest;
import io.confluent.kafkarest.extension.ResourceAccesslistFeature.ResourceName;
import io.confluent.kafkarest.resources.AsyncResponses;
import io.confluent.kafkarest.response.CrnFactory;
import io.confluent.kafkarest.response.UrlFactory;
import io.confluent.rest.annotations.PerformanceMetric;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Path("/v3/clusters/{clusterId}/topics/{topicName}/consume")
@ResourceName("api.v3.consume.*")
public final class TopicConsumeAction {

  private final Provider<ConsumeManager> consumeManager;
  private final CrnFactory crnFactory;
  private final UrlFactory urlFactory;

  @Inject
  public TopicConsumeAction(
      Provider<ConsumeManager> consumeManager,
      CrnFactory crnFactory,
      UrlFactory urlFactory
  ) {
    this.consumeManager = requireNonNull(consumeManager);
    this.crnFactory = requireNonNull(crnFactory);
    this.urlFactory = requireNonNull(urlFactory);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @PerformanceMetric("v3.consume.list")
  @ResourceName("api.v3.consume.list")
  public void listConsumeRecords(
      @Suspended AsyncResponse asyncResponse,
      @PathParam("clusterId") String clusterId,
      @PathParam("topicName") String topicName,
      @DefaultValue("BINARY") @PathParam("format") String format,
      @DefaultValue("-1") @QueryParam("timestamp") Long timestamp,
      @DefaultValue("1")  @QueryParam("page_size") Integer pageSize,
      @Valid TopicConsumeRequest request
  ) {
    CompletableFuture<ListConsumeRecordsResponse> response = null;
    if (timestamp != -1L) {
      response = consumeManager.get()
              .getRecords(clusterId,
                  topicName,
                  Optional.empty(),
                  Optional.of(timestamp),
                  pageSize)
              .thenApply(
                  records ->
                      ListConsumeRecordsResponse.create(
                          ConsumeRecordDataList.builder()
                              .setMetadata(
                                  ResourceCollection.Metadata.builder()
                                      .setSelf(
                                          urlFactory.create(
                                              "v3",
                                              "clusters",
                                              clusterId,
                                              "topic_name",
                                              topicName,
                                              "consume"))
                                      .build())
                              .setData(
                                  records.stream()
                                      .map(record -> toConsumeRecordData(record,
                                          EmbeddedFormat.valueOf(format)))
                                      .collect(Collectors.toList()))
                              .build()));
    } else {
      response = consumeManager.get()
              .getRecords(clusterId,
                  topicName,
                  Optional.of(request.getValue().getData().stream().map(
                    entry -> new AbstractMap.SimpleEntry<Integer,Long>(
                        entry.getPartitionId(),entry.getOffset())
                  ).collect(Collectors.toMap(Map.Entry::getKey,Map.Entry::getValue))),
                  Optional.empty(),
                  pageSize)
              .thenApply(
                  records ->
                      ListConsumeRecordsResponse.create(
                          ConsumeRecordDataList.builder()
                              .setMetadata(
                                  ResourceCollection.Metadata.builder()
                                      .setSelf(
                                          urlFactory.create(
                                              "v3",
                                              "clusters",
                                              clusterId,
                                              "topic_name",
                                              topicName,
                                              "consume"))
                                      .build())
                              .setData(
                                  records.stream()
                                      .map(record -> toConsumeRecordData(record,
                                          EmbeddedFormat.valueOf(format)))
                                      .collect(Collectors.toList()))
                              .build()));
    }

    AsyncResponses.asyncResume(asyncResponse, response);
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
                    crnFactory.create(
                        "kafka",
                        record.getClusterId(),
                        "topic_name",
                        record.getTopicName(),
                        "partition_id",
                        Integer.toString(record.getPartition()),
                        "offset",
                        Long.toString(record.getOffset())))
                .build())
        .build();
  }
}
