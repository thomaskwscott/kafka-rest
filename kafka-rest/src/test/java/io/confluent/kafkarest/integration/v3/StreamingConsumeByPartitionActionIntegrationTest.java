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

package io.confluent.kafkarest.integration.v3;

import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest.BinaryPartitionProduceRecord;

import io.confluent.kafkarest.integration.ClusterTestHarness;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.sse.InboundSseEvent;
import javax.ws.rs.sse.SseEventSource;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class StreamingConsumeByPartitionActionIntegrationTest extends ClusterTestHarness {

  private static final String topic1 = "topic-1";
  private String baseUrl;
  private String clusterId;

  private final List<BinaryPartitionProduceRecord> partitionRecords =
      Arrays.asList(
          new BinaryPartitionProduceRecord("key", "value"),
          new BinaryPartitionProduceRecord("key2", "value2"),
          new BinaryPartitionProduceRecord("key3", "value3")
      );

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    baseUrl = restConnect;
    clusterId = getClusterId();
    createTopic(topic1, 1, (short) 1);
  }

  @Test
  public void streamConsumeRecords_returnsConsumeRecords() throws InterruptedException {
    BinaryPartitionProduceRequest request1 =
        BinaryPartitionProduceRequest.create(partitionRecords);
    produce(topic1, 0, request1);

    Client client = ClientBuilder.newBuilder()
        .connectTimeout(2, TimeUnit.SECONDS)
        .readTimeout(2,TimeUnit.SECONDS)
        .build();
    WebTarget target = sseTarget("/v3/clusters/" + clusterId + "/topics/" + topic1 + "/partitions/0/stream",
        new HashMap<String,String>(){{
          put("offset", "0");
        }},client);

    List<InboundSseEvent> inboundEvents = new ArrayList<>();
    try (SseEventSource source = SseEventSource.target(target).build()) {
      source.register(inboundSseEvent -> {
        System.out.println(inboundSseEvent);
        inboundEvents.add(inboundSseEvent);
      });
      source.open();

      while (inboundEvents.size() < 3) {
        //Consuming events
        Thread.sleep(1000);
      }

    } catch (Exception e) {
      fail();
    }

    client.close();
  }

  private void produce(String topicName, int partitionId, BinaryPartitionProduceRequest request) {
    request("topics/" + topicName + "/partitions/" + partitionId, Collections.emptyMap())
        .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));
  }

}
