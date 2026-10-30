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
import com.datastax.oss.common.sink.config.AuthenticatorConfig;
import com.datastax.oss.common.sink.schema.FatalSchemaException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@CCMConfig(
  config = {"authenticator:PasswordAuthenticator", "authorizer:CassandraAuthorizer"},
  jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0"
)
@Tag("medium")
class SchemaManagementCCMIT extends EndToEndCCMITBase {
  private static final AtomicInteger KEYSPACE_COUNTER = new AtomicInteger();
  private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();
  private static final String SUPERUSER = "cassandra";
  private static final String WRITER = "writer";

  SchemaManagementCCMIT(
      CCMCluster ccm, @SessionConfig(credentials = {"cassandra", "cassandra"}) CqlSession session) {
    super(ccm, session);
  }

  private static String newTableName() {
    return "auto_table_" + TABLE_COUNTER.incrementAndGet();
  }

  private static Map<String, String> withCredentials(String role, Map<String, String> extras) {
    Map<String, String> props = new HashMap<>(extras);
    props.put(AuthenticatorConfig.USERNAME_OPT, role);
    props.put(AuthenticatorConfig.PASSWORD_OPT, role);
    return props;
  }

  private static String newKeyspaceName() {
    return "auto_ks_" + KEYSPACE_COUNTER.incrementAndGet();
  }

  /** Properties binding mytopic to table "types" in the given keyspace. */
  private Map<String, String> propertiesFor(String keyspace, Map<String, String> extras) {
    Map<String, String> props =
        makeConnectorProperties("bigintcol=value.bigint", withCredentials(SUPERUSER, extras));
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

  private Optional<TableMetadata> tableMetadata(String table) {
    return session
        .refreshSchema()
        .getKeyspace(CqlIdentifier.fromInternal(keyspaceName))
        .flatMap(ks -> ks.getTable(CqlIdentifier.fromInternal(table)));
  }

  @Test
  void should_create_table_on_first_record_and_write_it() {
    String table = newTableName();
    Map<String, String> extras = new HashMap<>();
    extras.put("createTable", "true");
    conn.start(
        makeConnectorProperties(
            "id=key, name=value.name, day=value.day, ts=value.ts, amount=value.amount",
            table,
            withCredentials(SUPERUSER, extras)));

    Schema valueSchema =
        SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("day", Date.SCHEMA)
            .field("ts", Timestamp.SCHEMA)
            .field("amount", Decimal.schema(2))
            .build();
    java.util.Date day = Date.toLogical(Date.SCHEMA, 19000);
    java.util.Date ts = new java.util.Date(1_700_000_000_123L);
    BigDecimal amount = new BigDecimal("12.34");
    Struct value =
        new Struct(valueSchema)
            .put("name", "widget")
            .put("day", day)
            .put("ts", ts)
            .put("amount", amount);
    runTaskWithRecords(
        new SinkRecord("mytopic", 0, Schema.INT64_SCHEMA, 42L, valueSchema, value, 1234L));

    TableMetadata created = tableMetadata(table).get();
    assertThat(created.getPartitionKey())
        .extracting(ColumnMetadata::getName)
        .containsExactly(CqlIdentifier.fromInternal("id"));
    assertThat(created.getColumn("id").get().getType()).isEqualTo(DataTypes.BIGINT);
    assertThat(created.getColumn("name").get().getType()).isEqualTo(DataTypes.TEXT);
    assertThat(created.getColumn("day").get().getType()).isEqualTo(DataTypes.DATE);
    assertThat(created.getColumn("ts").get().getType()).isEqualTo(DataTypes.TIMESTAMP);
    assertThat(created.getColumn("amount").get().getType()).isEqualTo(DataTypes.DECIMAL);

    Row row = session.execute("SELECT * FROM " + table).one();
    assertThat(row.getLong("id")).isEqualTo(42L);
    assertThat(row.getString("name")).isEqualTo("widget");
    assertThat(row.getLocalDate("day")).isEqualTo(LocalDate.ofEpochDay(19000));
    assertThat(row.getInstant("ts")).isEqualTo(ts.toInstant());
    assertThat(row.getBigDecimal("amount")).isEqualTo(amount);
  }

  @Test
  void should_fail_task_on_privilege_error_under_ignore_errors_all() {
    String keyspace = CqlIdentifier.fromInternal(keyspaceName).asCql(true);
    session.execute(
        String.format(
            "CREATE ROLE IF NOT EXISTS %s WITH PASSWORD = '%s' AND LOGIN = true", WRITER, WRITER));
    session.execute(String.format("GRANT SELECT ON KEYSPACE %s TO %s", keyspace, WRITER));
    session.execute(String.format("GRANT MODIFY ON KEYSPACE %s TO %s", keyspace, WRITER));
    String table = newTableName();
    Map<String, String> extras = new HashMap<>();
    extras.put("createTable", "true");
    extras.put("ignoreErrors", "All");
    conn.start(
        makeConnectorProperties("id=key, name=value.name", table, withCredentials(WRITER, extras)));

    Schema valueSchema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    SinkRecord record =
        new SinkRecord(
            "mytopic",
            0,
            Schema.INT64_SCHEMA,
            1L,
            valueSchema,
            new Struct(valueSchema).put("name", "widget"),
            1234L);

    assertThatThrownBy(() -> runTaskWithRecords(record))
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(FatalSchemaException.class)
        .hasStackTraceContaining("CREATE TABLE");
    assertThat(tableMetadata(table)).isEmpty();
  }

  private Map<String, String> implicitProperties(String keyspace, String topics) {
    Map<String, String> extras = new HashMap<>();
    extras.put("mappingMode", "IMPLICIT");
    extras.put("keyspace", keyspace);
    Map<String, String> props =
        makeConnectorProperties("unused=value", "unused", withCredentials(SUPERUSER, extras));
    props.remove(String.format("topic.mytopic.%s.unused.mapping", keyspaceName));
    props.put("topics", topics);
    return props;
  }

  @Test
  void implicit_hands_off_creates_keyspace_and_tables_per_topic() {
    String keyspace = newKeyspaceName();
    Map<String, String> props = implicitProperties(keyspace, "orders,orders-v1.raw");
    props.put("createKeyspace", "true");
    props.put("keyspaceReplicationFactor", "1");
    props.put("createTable", "true");
    conn.start(props);

    Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();
    runTaskWithRecords(
        new SinkRecord(
            "orders",
            0,
            keySchema,
            new Struct(keySchema).put("id", 1L),
            valueSchema,
            new Struct(valueSchema).put("id", 1L).put("name", "plain"),
            1L),
        new SinkRecord(
            "orders-v1.raw",
            0,
            keySchema,
            new Struct(keySchema).put("id", 2L),
            valueSchema,
            new Struct(valueSchema).put("id", 2L).put("name", "dotted"),
            2L));

    KeyspaceMetadata created =
        session.refreshSchema().getKeyspace(CqlIdentifier.fromInternal(keyspace)).get();
    for (String topic : new String[] {"orders", "orders-v1.raw"}) {
      TableMetadata table = created.getTable(CqlIdentifier.fromInternal(topic)).get();
      assertThat(table.getPartitionKey())
          .extracting(ColumnMetadata::getName)
          .containsExactly(CqlIdentifier.fromInternal("id"));
      assertThat(table.getColumns()).hasSize(2);
    }
    Row plain = session.execute(String.format("SELECT * FROM %s.orders", keyspace)).one();
    assertThat(plain.getLong("id")).isEqualTo(1L);
    assertThat(plain.getString("name")).isEqualTo("plain");
    Row dotted =
        session.execute(String.format("SELECT * FROM %s.\"orders-v1.raw\"", keyspace)).one();
    assertThat(dotted.getLong("id")).isEqualTo(2L);
    assertThat(dotted.getString("name")).isEqualTo("dotted");
  }

  @Test
  void implicit_with_mapping_fails_start() {
    Map<String, String> props = propertiesFor(keyspaceName, Collections.emptyMap());
    props.put("mappingMode", "IMPLICIT");
    props.put("keyspace", keyspaceName);
    conn.start(props);

    assertThatThrownBy(() -> task.start(conn.taskConfigs(1).get(0)))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("mapping must not be set when mappingMode=IMPLICIT")
        .hasMessageContaining("mappingMode=EXPLICIT");
  }
}
