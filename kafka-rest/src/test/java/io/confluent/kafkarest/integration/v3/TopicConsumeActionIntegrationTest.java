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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.*;

import static io.confluent.kafkarest.TestUtils.testWithRetry;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class TopicConsumeActionIntegrationTest extends ClusterTestHarness {

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
    createTopic(topic1, 2, (short) 1);
  }

  @Test
  public void listConsumeRecords_byTopic_returnsConsumeRecords() {
    // produce to topic1 partition0 and topic2 partition1
    BinaryPartitionProduceRequest request1 =
        BinaryPartitionProduceRequest.create(partitionRecords);
    long timestamp = System.currentTimeMillis();
    produce(topic1, 0, request1);
    produce(topic1, 1, request1);


    Response timestampResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topic1 + "/consume",
            new HashMap<String,String>(){{
              put("timestamp", String.valueOf(timestamp));
              put("page_size", "6");
            }})
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity("",MediaType.APPLICATION_JSON));

    assertEquals(Status.OK.getStatusCode(), timestampResponse.getStatus());
    ConsumeRecordDataList timestampConsumeRecordDataList =
        timestampResponse.readEntity(ListConsumeRecordsResponse.class).getValue();
    System.out.println(timestampConsumeRecordDataList);

    TopicConsumeRequestData topicConsumeRequestData = TopicConsumeRequestData.create(
      Arrays.asList(
          TopicConsumeRequestData.PartitionOffsetEntry.builder()
              .setOffset(0L)
              .setPartitionId(0)
              .build(),
          TopicConsumeRequestData.PartitionOffsetEntry.builder()
              .setOffset(0L)
              .setPartitionId(1)
              .build()
      )
    );

    // {"value":{"data":[{"partitionId":0,"offset":0},{"partitionId":1,"offset":0}]}}
    TopicConsumeRequest request = TopicConsumeRequest.create(topicConsumeRequestData);
    Response offsetResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topic1 + "/consume",
            new HashMap<String,String>(){{
              put("page_size", "6");
            }})
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.json(request));

    assertEquals(Status.OK.getStatusCode(), offsetResponse.getStatus());
    ConsumeRecordDataList offsetConsumeRecordDataList =
        offsetResponse.readEntity(ListConsumeRecordsResponse.class).getValue();
    System.out.println(offsetConsumeRecordDataList);

  }

  private void produce(String topicName, int partitionId, BinaryPartitionProduceRequest request) {
    request("topics/" + topicName + "/partitions/" + partitionId, Collections.emptyMap())
        .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));
  }

}
