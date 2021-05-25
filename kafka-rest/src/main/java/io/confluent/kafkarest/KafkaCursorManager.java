/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafkarest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class KafkaCursorManager {

  public static final String REST_PROXY_CURSORS = "rest-proxy-cursors";
  public static final String REST_PROXY_CURSOR_STORE = "rest-proxy-cursor-store";

  private static final Logger log = LoggerFactory.getLogger(KafkaCursorManager.class);

  private final KafkaRestConfig config;
  public final KafkaStreams streams;


  public KafkaCursorManager(final KafkaRestConfig config) {
    this.config = config;

    Properties streamsProperties = new Properties();
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,"rest-proxy-streams-test");
    streamsProperties.putAll(config.getOriginalProperties());

    final StreamsBuilder builder = new StreamsBuilder();

    // Create a global table for cursors. The data from this global table
    // will be fully replicated on each instance of this application.

    final GlobalKTable<String,String>
        cursors =
        builder.globalTable(REST_PROXY_CURSORS,
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(REST_PROXY_CURSOR_STORE)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.String())
            .withRetention(Duration.ofHours(4L))
        );

    streams = new KafkaStreams(builder.build(), streamsProperties);
    streams.start();
  }

  public String getCursorPosition(String cursor) {
    Object fetched = null;
    System.out.println("start fetch time: " + System.currentTimeMillis());
    //while (fetched == null) {
      fetched = streams.store(StoreQueryParameters.fromNameAndType("rest-proxy-cursor-store",
          QueryableStoreTypes.keyValueStore()
      )).get(cursor);
    //}
    System.out.println("end fetch time: " + System.currentTimeMillis());
    return fetched.toString();
  }

  public void setCursorPosition(String cursor,String position) {
    Properties producerProps = new Properties();
    producerProps.putAll(config.getProducerConfigs());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    Producer<String,String> producer = new KafkaProducer<>(producerProps);
    ProducerRecord<String,String> record = new ProducerRecord<>(REST_PROXY_CURSORS,cursor,position);
    try {
      producer.send(record).get();
    } catch (Exception e) {
      log.error("Couldn't update cursor store",e);
    }
  }

}
