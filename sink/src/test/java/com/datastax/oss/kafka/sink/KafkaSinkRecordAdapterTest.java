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

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class KafkaSinkRecordAdapterTest {

  // CC-45025: toString() must expose only the record's coordinates, never the key or value.
  @Test
  void toString_omits_key_and_value() {
    SinkRecord record = new SinkRecord("orders", 3, null, "SECRET_KEY", null, "SECRET_VALUE", 42L);

    String rendered = new KafkaSinkRecordAdapter(record).toString();

    assertThat(rendered).contains("topic=orders", "partition=3", "offset=42");
    assertThat(rendered).doesNotContain("SECRET_KEY", "SECRET_VALUE");
  }
}
