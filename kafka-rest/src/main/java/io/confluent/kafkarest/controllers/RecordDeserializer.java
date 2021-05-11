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

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafkarest.entities.EmbeddedFormat;

import java.util.Optional;

/**
 * An interface to convert all {@link EmbeddedFormat formats} to Json.
 */
public interface RecordDeserializer {

  /**
   * Deserializes the given {@code data} into a {@link JsonNode}.
   *
   * <p>Returns {@link Optional#empty()} if {@code data} is empty.
   */
  Optional<JsonNode> deserialize(
      byte[] data);
}
