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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import java.util.Optional;

public class BinaryDeserializer implements RecordDeserializer {

  @Override
  public Optional<JsonNode> deserialize(byte[] data) {
    JsonNode node = null;
    try {
      node = new ObjectMapper().readTree(
          "{ \"base64Value\":\"" + BaseEncoding.base64().encode(data) + "\"}"
      );
    } catch (JsonProcessingException e) {
      return Optional.empty();
    }
    return Optional.of(node);
  }
}
