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
import static org.mockito.Mockito.mock;

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
import com.datastax.oss.kafka.sink.CassandraSinkTask;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
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
    Map<String, String> props = implicitProperties(keyspace, "orders,Payments");
    props.put("createKeyspace", "true");
    props.put("keyspaceReplicationFactor", "1");
    props.put("createTable", "true");
    conn.start(props);

    Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("customerName", Schema.STRING_SCHEMA)
            .build();
    runTaskWithRecords(
        new SinkRecord(
            "orders",
            0,
            keySchema,
            new Struct(keySchema).put("id", 1L),
            valueSchema,
            new Struct(valueSchema).put("id", 1L).put("customerName", "first"),
            1L),
        new SinkRecord(
            "Payments",
            0,
            keySchema,
            new Struct(keySchema).put("id", 2L),
            valueSchema,
            new Struct(valueSchema).put("id", 2L).put("customerName", "second"),
            2L));

    // Unquoted CQL naming: table and column names are lower-cased.
    KeyspaceMetadata created =
        session.refreshSchema().getKeyspace(CqlIdentifier.fromInternal(keyspace)).get();
    for (String table : new String[] {"orders", "payments"}) {
      TableMetadata metadata = created.getTable(CqlIdentifier.fromInternal(table)).get();
      assertThat(metadata.getPartitionKey())
          .extracting(ColumnMetadata::getName)
          .containsExactly(CqlIdentifier.fromInternal("id"));
      assertThat(metadata.getColumns())
          .containsOnlyKeys(
              CqlIdentifier.fromInternal("id"), CqlIdentifier.fromInternal("customername"));
    }
    Row first = session.execute(String.format("SELECT * FROM %s.orders", keyspace)).one();
    assertThat(first.getLong("id")).isEqualTo(1L);
    assertThat(first.getString("customername")).isEqualTo("first");
    Row second = session.execute(String.format("SELECT * FROM %s.payments", keyspace)).one();
    assertThat(second.getLong("id")).isEqualTo(2L);
    assertThat(second.getString("customername")).isEqualTo("second");
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

  private Map<String, String> evolvingProperties(String topic) {
    Map<String, String> props = implicitProperties(keyspaceName, topic);
    props.put("createTable", "true");
    props.put("evolveTable", "true");
    return props;
  }

  private static Schema evolveKeySchema() {
    return SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
  }

  private static Schema evolveValueSchema(boolean withEmail) {
    SchemaBuilder builder =
        SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).field("name", Schema.STRING_SCHEMA);
    if (withEmail) {
      builder.field("email", Schema.OPTIONAL_STRING_SCHEMA);
    }
    return builder.build();
  }

  private static SinkRecord evolveRecord(String topic, long id, Schema valueSchema) {
    Schema keySchema = evolveKeySchema();
    Struct value = new Struct(valueSchema).put("id", id).put("name", "n" + id);
    if (valueSchema.field("email") != null) {
      value.put("email", id + "@example.com");
    }
    return new SinkRecord(
        topic, 0, keySchema, new Struct(keySchema).put("id", id), valueSchema, value, id);
  }

  private Row rowById(String table, long id) {
    return session.execute(String.format("SELECT * FROM %s WHERE id = %d", table, id)).one();
  }

  @Test
  void should_evolve_mid_stream() {
    String topic = newTableName();
    conn.start(evolvingProperties(topic));

    runTaskWithRecords(evolveRecord(topic, 1L, evolveValueSchema(false)));
    task.put(Collections.singletonList(evolveRecord(topic, 2L, evolveValueSchema(true))));

    assertThat(tableMetadata(topic).get().getColumn("email").get().getType())
        .isEqualTo(DataTypes.TEXT);
    assertThat(rowById(topic, 1L).isNull("email")).isTrue();
    assertThat(rowById(topic, 2L).getString("email")).isEqualTo("2@example.com");
  }

  private static SinkRecord tombstone(String topic, long id) {
    Schema keySchema = evolveKeySchema();
    return new SinkRecord(topic, 0, keySchema, new Struct(keySchema).put("id", id), null, null, id);
  }

  @Test
  void implicit_rejects_topic_that_is_not_a_valid_table_name() {
    conn.start(implicitProperties(keyspaceName, "orders-v1.raw"));

    assertThatThrownBy(() -> task.start(conn.taskConfigs(1).get(0)))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Topic 'orders-v1.raw' cannot be used with mappingMode=IMPLICIT");
  }

  @Test
  void implicit_deletes_on_tombstone_first_after_restart() {
    String topic = newTableName();
    conn.start(evolvingProperties(topic));
    runTaskWithRecords(
        evolveRecord(topic, 1L, evolveValueSchema(false)),
        evolveRecord(topic, 2L, evolveValueSchema(false)));

    CassandraSinkTask restarted = new CassandraSinkTask();
    restarted.initialize(mock(SinkTaskContext.class));
    restarted.start(conn.taskConfigs(1).get(0));
    try {
      restarted.put(Collections.singletonList(tombstone(topic, 1L)));
    } finally {
      restarted.stop();
    }

    assertThat(rowById(topic, 1L)).isNull();
    assertThat(rowById(topic, 2L)).isNotNull();
  }

  @Test
  void implicit_skips_tombstone_when_table_does_not_exist() {
    String topic = newTableName();
    conn.start(evolvingProperties(topic));

    runTaskWithRecords(tombstone(topic, 1L));

    assertThat(tableMetadata(topic)).isEmpty();
  }

  @Test
  void implicit_keeps_row_on_all_null_record_and_deletes_on_tombstone() {
    String topic = newTableName();
    conn.start(evolvingProperties(topic));
    runTaskWithRecords(evolveRecord(topic, 1L, evolveValueSchema(true)));

    Schema older =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    Schema keySchema = evolveKeySchema();
    task.put(
        Collections.singletonList(
            new SinkRecord(
                topic,
                0,
                keySchema,
                new Struct(keySchema).put("id", 1L),
                older,
                new Struct(older).put("id", 1L),
                2L)));

    Row kept = rowById(topic, 1L);
    assertThat(kept.getString("name")).isEqualTo("n1");
    assertThat(kept.getString("email")).isEqualTo("1@example.com");

    task.put(Collections.singletonList(tombstone(topic, 1L)));
    assertThat(rowById(topic, 1L)).isNull();
  }

  @Test
  void two_tasks_evolve_same_table_without_failures() throws Exception {
    String topic = newTableName();
    conn.start(evolvingProperties(topic));
    runTaskWithRecords(evolveRecord(topic, 1L, evolveValueSchema(false)));
    CassandraSinkTask second = new CassandraSinkTask();
    second.initialize(mock(SinkTaskContext.class));
    second.start(conn.taskConfigs(2).get(1));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      CountDownLatch start = new CountDownLatch(1);
      // Each task has its own, equal v2 schema instance, as with per-task converters.
      Future<?> first =
          executor.submit(
              () -> {
                start.await();
                task.put(
                    Collections.singletonList(evolveRecord(topic, 2L, evolveValueSchema(true))));
                return null;
              });
      Future<?> other =
          executor.submit(
              () -> {
                start.await();
                second.put(
                    Collections.singletonList(evolveRecord(topic, 3L, evolveValueSchema(true))));
                return null;
              });
      start.countDown();
      first.get(60, TimeUnit.SECONDS);
      other.get(60, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
      second.stop();
    }

    assertThat(tableMetadata(topic).get().getColumn("email").get().getType())
        .isEqualTo(DataTypes.TEXT);
    assertThat(rowById(topic, 2L).getString("email")).isEqualTo("2@example.com");
    assertThat(rowById(topic, 3L).getString("email")).isEqualTo("3@example.com");
  }
}
