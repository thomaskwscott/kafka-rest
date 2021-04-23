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

package io.confluent.kafkarest;

import org.apache.kafka.clients.consumer.Consumer;

import static java.util.Objects.requireNonNull;

public final class ConsumerPool {

  private final Consumer<byte[], byte[]> consumer;

  public ConsumerPool(Consumer<byte[], byte[]> consumer) {
    this.consumer = requireNonNull(consumer);
  }

  public Consumer<byte[], byte[]> getConsumer() {
    return consumer;
  }
}
