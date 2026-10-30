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
package com.datastax.oss.kafka.sink.ccm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.common.sink.ConfigException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class SchemaManagementCCMIT extends EndToEndCCMITBase {
  private static final AtomicInteger KEYSPACE_COUNTER = new AtomicInteger();

  SchemaManagementCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  private static String newKeyspaceName() {
    return "auto_ks_" + KEYSPACE_COUNTER.incrementAndGet();
  }

  /** Properties binding mytopic to table "types" in the given keyspace. */
  private Map<String, String> propertiesFor(String keyspace, Map<String, String> extras) {
    Map<String, String> props = makeConnectorProperties("bigintcol=value.bigint", extras);
    props.remove(String.format("topic.mytopic.%s.types.mapping", keyspaceName));
    props.put(String.format("topic.mytopic.%s.types.mapping", keyspace), "bigintcol=value.bigint");
    return props;
  }

  @Test
  void should_create_missing_keyspace_with_replication_factor() {
    String keyspace = newKeyspaceName();
    Map<String, String> extras = new HashMap<>();
    extras.put("createKeyspace", "true");
    extras.put("keyspaceReplicationFactor", "1");
    conn.start(propertiesFor(keyspace, extras));

    // the table is not created here, so start fails after the keyspace has been created
    assertThatThrownBy(() -> task.start(conn.taskConfigs(1).get(0)))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Table types does not exist");

    KeyspaceMetadata created =
        session.getMetadata().getKeyspace(CqlIdentifier.fromInternal(keyspace)).get();
    assertThat(created.getReplication())
        .containsEntry("class", "org.apache.cassandra.locator.SimpleStrategy")
        .containsEntry("replication_factor", "1");
  }

  @Test
  void should_fail_start_when_keyspace_missing_and_flag_off() {
    String keyspace = newKeyspaceName();
    conn.start(propertiesFor(keyspace, Collections.emptyMap()));

    assertThatThrownBy(() -> task.start(conn.taskConfigs(1).get(0)))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(String.format("Keyspace %s does not exist.", keyspace));
    assertThat(session.getMetadata().getKeyspace(CqlIdentifier.fromInternal(keyspace))).isEmpty();
  }
}
