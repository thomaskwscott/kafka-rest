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

package io.confluent.kafkarest.entities;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafkarest.controllers.BinaryDeserializer;
import io.confluent.kafkarest.controllers.RecordDeserializer;

/**
 * Permitted formats for ProduceRecords embedded in produce requests/consume responses, e.g.
 * base64-encoded binary, JSON-encoded Avro, etc. Each of these correspond to a content type, a
 * ProduceRecord implementation, a Producer in the ProducerPool (with corresponding Kafka
 * serializer), ConsumerRecord implementation, and a serializer for any instantiated consumers.
 *
 * <p>Note that for each type, it's assumed that the key and value can be handled by the same
 * serializer. This means each serializer should handle both it's complex type (e.g.
 * Indexed/Generic/SpecificRecord for Avro) and boxed primitive types (Integer, Boolean, etc.).
 */
public enum EmbeddedFormat {
  BINARY {
    @Override
    public boolean requiresSchema() {
      return false;
    }

    @Override
    public SchemaProvider getSchemaProvider() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RecordDeserializer getKeyDeserializer() {
      return new BinaryDeserializer();
    }

    @Override
    public RecordDeserializer getValueDeserializer() {
      return new BinaryDeserializer();
    }
  },

  JSON {
    @Override
    public boolean requiresSchema() {
      return false;
    }

    @Override
    public SchemaProvider getSchemaProvider() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RecordDeserializer getKeyDeserializer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RecordDeserializer getValueDeserializer() {
      throw new UnsupportedOperationException();
    }
  },

  AVRO {
    private final SchemaProvider schemaProvider = new AvroSchemaProvider();

    @Override
    public boolean requiresSchema() {
      return true;
    }

    @Override
    public SchemaProvider getSchemaProvider() {
      return schemaProvider;
    }

    @Override
    public RecordDeserializer getKeyDeserializer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RecordDeserializer getValueDeserializer() {
      throw new UnsupportedOperationException();
    }
  },

  JSONSCHEMA {
    private final SchemaProvider schemaProvider = new JsonSchemaProvider();

    @Override
    public boolean requiresSchema() {
      return true;
    }

    @Override
    public SchemaProvider getSchemaProvider() {
      return schemaProvider;
    }

    @Override
    public RecordDeserializer getKeyDeserializer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RecordDeserializer getValueDeserializer() {
      throw new UnsupportedOperationException();
    }
  },

  PROTOBUF {
    private final SchemaProvider schemaProvider = new ProtobufSchemaProvider();

    @Override
    public boolean requiresSchema() {
      return true;
    }

    @Override
    public SchemaProvider getSchemaProvider() {
      return schemaProvider;
    }

    @Override
    public RecordDeserializer getKeyDeserializer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RecordDeserializer getValueDeserializer() {
      throw new UnsupportedOperationException();
    }
  };

  public abstract boolean requiresSchema();

  public abstract SchemaProvider getSchemaProvider();

  public abstract RecordDeserializer getKeyDeserializer();

  public abstract RecordDeserializer getValueDeserializer();

  public static EmbeddedFormat forSchemaType(String schemaType) {
    if (schemaType.equals(AVRO.getSchemaProvider().schemaType())) {
      return AVRO;
    } else if (schemaType.equals(JSONSCHEMA.getSchemaProvider().schemaType())) {
      return JSONSCHEMA;
    } else if (schemaType.equals(PROTOBUF.getSchemaProvider().schemaType())) {
      return PROTOBUF;
    } else {
      throw new IllegalArgumentException(String.format("Illegal schema type: %s", schemaType));
    }
  }
}
