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

import com.avast.huffman.HuffmanCompressor;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest;
import io.confluent.kafkarest.entities.v2.BinaryPartitionProduceRequest.BinaryPartitionProduceRecord;
import io.confluent.kafkarest.entities.v3.*;
import io.confluent.kafkarest.integration.ClusterTestHarness;
import nayuki.huffmancoding.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.xerial.snappy.Snappy;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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
  public void compress_consumeNextToken() throws IOException {
    Random random = new Random();
    Map<Integer,Long> positions = new HashMap<>();
    for( int i=0; i<1000; i++)
    {
      positions.put(i,100000L + (i%500) );
    }
    ConsumeNextToken token = ConsumeNextToken.builder()
        .setPosition(positions).build();
    Gson gson = new GsonBuilder().create();
    System.out.println(token.getPosition());
    System.out.println(deserializeConsumeNextToken(serializeConsumeNextToken(token)).getPosition());
    System.out.println("Original: " + serializeConsumeNextToken(token).length);
    System.out.println("snappy: " + Snappy.compress(serializeConsumeNextToken(token)).length);
    System.out.println("deflate: " + compress(serializeConsumeNextToken(token)).length);
    System.out.println("gzip: " + gzip(serializeConsumeNextToken(token)).length);
    System.out.println("huffman: " + huffmanCompress(serializeConsumeNextToken(token)).length);
    System.out.println("Original base64: " + Base64.getEncoder().encodeToString(serializeConsumeNextToken(token)).length());
    System.out.println("snappy base64: " + Base64.getEncoder().encodeToString(Snappy.compress(serializeConsumeNextToken(token))).length());
    System.out.println("deflate base64: " + Base64.getEncoder().encodeToString(compress(serializeConsumeNextToken(token))).length());
    System.out.println("gzip base64: " + Base64.getEncoder().encodeToString(gzip(serializeConsumeNextToken(token))).length());
    System.out.println("huffman base64: " + Base64.getEncoder().encodeToString(huffmanCompress(serializeConsumeNextToken(token))).length());
  }

  protected byte[] huffmanCompress(byte[] b) throws IOException {
    InputStream in = new ByteArrayInputStream(b);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BitOutputStream bitOut = new BitOutputStream(out);

    FrequencyTable freq = getFrequencies(b);
    CodeTree code = freq.buildCodeTree();
    CanonicalCode canonCode = new CanonicalCode(code, 257);
    code = canonCode.toCodeTree();
    HuffmanCompress.compress(code, in, bitOut);
    bitOut.close();
    return out.toByteArray();
  }

  private static FrequencyTable getFrequencies(byte[] b) {
    FrequencyTable freq = new FrequencyTable(new int[257]);
    for (byte x : b)
      freq.increment(x & 0xFF);
    freq.increment(256);  // EOF symbol gets a frequency of 1
    return freq;
  }

  public static byte[] gzip(byte[] val) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(val.length);
    GZIPOutputStream gos = null;
    try {
      gos = new GZIPOutputStream(bos);
      gos.write(val, 0, val.length);
      gos.finish();
      gos.flush();
      bos.flush();
      val = bos.toByteArray();
    } finally {
      if (gos != null)
        gos.close();
      if (bos != null)
        bos.close();
    }
    return val;
  }

  private byte[] compress(byte[] in) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DeflaterOutputStream defl = new DeflaterOutputStream(out);
      defl.write(in);
      defl.flush();
      defl.close();

      return out.toByteArray();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(150);
      return null;
    }
  }

  private byte[] serializeConsumeNextToken(ConsumeNextToken token) {
    int size = token.getPosition().keySet().size();
    ByteBuffer buf = ByteBuffer.allocate(size * Long.BYTES);
    for( int i=0; i<size; i++)
    {
      buf.putLong(token.getPosition().get(i));
    }
    return buf.array();
  }


  private ConsumeNextToken deserializeConsumeNextToken(byte[] in) {
    Map<Integer,Long> positions = new HashMap<>();
    byte[] serialized = in;
    int size = serialized.length/Long.BYTES;
    for( int i=0; i<size; i++)
    {
      byte[] oneVal = Arrays.copyOfRange(serialized,i * Long.BYTES,((i+1) * Long.BYTES) );
      ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
      buf.put(oneVal, 0, oneVal.length);
      buf.flip();//need flip
      positions.put(i,buf.getLong());
    }
    return ConsumeNextToken.builder().setPosition(positions).build();
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

    Response tokenResponse =
        request("/v3/clusters/" + clusterId + "/topics/" + topic1 + "/partitions/0/consume",
            new HashMap<String,String>(){{
              put("page_size", "5");
            }})
            .accept(MediaType.APPLICATION_JSON)
            .post(Entity.entity(offsetConsumeRecordDataList.getNextToken(), Versions.KAFKA_V2_JSON_BINARY));

    assertEquals(Status.OK.getStatusCode(), tokenResponse.getStatus());
    ConsumeRecordDataList tokenConsumeRecordDataList =
        tokenResponse.readEntity(ListConsumeRecordsResponse.class).getValue();
    System.out.println(tokenConsumeRecordDataList);

  }

  private void produce(String topicName, int partitionId, BinaryPartitionProduceRequest request) {
    request("topics/" + topicName + "/partitions/" + partitionId, Collections.emptyMap())
        .post(Entity.entity(request, Versions.KAFKA_V2_JSON_BINARY));
  }

}
