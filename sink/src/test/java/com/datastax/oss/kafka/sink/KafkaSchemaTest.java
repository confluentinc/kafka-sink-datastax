/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.kafka.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.common.sink.AbstractSchema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class KafkaSchemaTest {

  @Test
  void should_preserve_logical_name_for_date() {
    assertThat(KafkaSchema.of(Date.SCHEMA).name()).isEqualTo(Date.LOGICAL_NAME);
    assertThat(KafkaSchema.of(Date.SCHEMA).type()).isEqualTo(AbstractSchema.Type.INT32);
  }

  @Test
  void should_preserve_logical_name_for_decimal_time_and_timestamp() {
    assertThat(KafkaSchema.of(Decimal.schema(2)).name()).isEqualTo(Decimal.LOGICAL_NAME);
    assertThat(KafkaSchema.of(Decimal.schema(2)).type()).isEqualTo(AbstractSchema.Type.BYTES);
    assertThat(KafkaSchema.of(Time.SCHEMA).name()).isEqualTo(Time.LOGICAL_NAME);
    assertThat(KafkaSchema.of(Timestamp.SCHEMA).name()).isEqualTo(Timestamp.LOGICAL_NAME);
    assertThat(KafkaSchema.of(Timestamp.SCHEMA).type()).isEqualTo(AbstractSchema.Type.INT64);
  }

  @Test
  void should_have_no_name_for_plain_primitives() {
    assertThat(KafkaSchema.of(Schema.INT32_SCHEMA).name()).isNull();
    assertThat(KafkaSchema.of(Schema.BYTES_SCHEMA).name()).isNull();
  }

  @Test
  void should_expose_underlying_schema_as_identity() {
    Schema s = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).build();
    assertThat(KafkaSchema.of(s).schemaIdentity()).isSameAs(KafkaSchema.of(s).schemaIdentity());
    assertThat(KafkaSchema.of(s).schemaIdentity()).isSameAs(s);
  }

  @Test
  void adapter_should_expose_key_and_value_schemas() {
    Schema s = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).build();
    Struct value = new Struct(s).put("a", "x");
    KafkaSinkRecordAdapter adapter =
        new KafkaSinkRecordAdapter(new SinkRecord("t", 0, Schema.INT64_SCHEMA, 1L, s, value, 0));
    assertThat(adapter.valueSchema().type()).isEqualTo(AbstractSchema.Type.STRUCT);
    assertThat(adapter.keySchema().type()).isEqualTo(AbstractSchema.Type.INT64);
  }

  @Test
  void adapter_should_return_null_schemas_for_schemaless_records() {
    KafkaSinkRecordAdapter adapter =
        new KafkaSinkRecordAdapter(new SinkRecord("t", 0, null, "k", null, "v", 0));
    assertThat(adapter.keySchema()).isNull();
    assertThat(adapter.valueSchema()).isNull();
  }

  @Test
  void adapter_should_return_null_identities_for_schemaless_records() {
    KafkaSinkRecordAdapter adapter =
        new KafkaSinkRecordAdapter(new SinkRecord("t", 0, null, "k", null, "v", 0));
    assertThat(adapter.keySchemaIdentity()).isNull();
    assertThat(adapter.valueSchemaIdentity()).isNull();
  }

  @Test
  void adapter_should_expose_raw_schemas_as_identities() {
    Schema s = SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).build();
    KafkaSinkRecordAdapter adapter =
        new KafkaSinkRecordAdapter(
            new SinkRecord("t", 0, Schema.INT64_SCHEMA, 1L, s, new Struct(s).put("a", "x"), 0));
    assertThat(adapter.keySchemaIdentity()).isSameAs(Schema.INT64_SCHEMA);
    assertThat(adapter.valueSchemaIdentity())
        .isSameAs(s)
        .isSameAs(adapter.valueSchema().schemaIdentity());
  }
}
