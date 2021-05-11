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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest.BinaryPartitionProduceRecord;
import io.confluent.kafkarest.entities.v3.*;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.time.Duration;
import java.util.*;

import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class PartitionConsumeActionIntegrationTest extends ClusterTestHarness {

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
  public void listConsumeRecords_byPartition_returnsConsumeRecords() {
    BinaryPartitionProduceRequest request1 =
        BinaryPartitionProduceRequest.create(partitionRecords);
    produce(topic1, 0, request1);

    Response offsetResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topic1 + "/partitions/0/consume",
            new HashMap<String,String>(){{
              put("offset", "0");
              put("page_size", "2");
            }})
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(null, Versions.KAFKA_V2_JSON_BINARY));

    assertEquals(Status.OK.getStatusCode(), offsetResponse.getStatus());
    ConsumeRecordDataList offsetConsumeRecordDataList =
        offsetResponse.readEntity(ListConsumeRecordsResponse.class).getValue();
    System.out.println(offsetConsumeRecordDataList);

    Response timestampResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topic1 + "/partitions/0/consume",
            new HashMap<String,String>(){{
              put("page_size", "5");
            }})
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(offsetConsumeRecordDataList.getNextToken(), Versions.KAFKA_V2_JSON_BINARY));

    assertEquals(Status.OK.getStatusCode(), timestampResponse.getStatus());
    ConsumeRecordDataList timeStampConsumeRecordDataList =
        timestampResponse.readEntity(ListConsumeRecordsResponse.class).getValue();
    System.out.println(timeStampConsumeRecordDataList);

  }

  private void produce(String topicName, int partitionId, BinaryPartitionProduceRequest request) {
    request("topics/" + topicName + "/partitions/" + partitionId, Collections.emptyMap())
        .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));
  }

}
