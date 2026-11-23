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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.common.sink.RecordMapper;
import com.datastax.oss.common.sink.config.CassandraSinkConfig;
import com.datastax.oss.common.sink.config.CassandraSinkConfig.IgnoreErrorsPolicy;
import com.datastax.oss.common.sink.config.TableConfig;
import com.datastax.oss.common.sink.config.TopicConfig;
import com.datastax.oss.common.sink.record.RecordAndStatement;
import com.datastax.oss.common.sink.state.InstanceState;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CassandraSinkTaskTest {
  private CassandraSinkTask sinkTask;
  private InstanceState instanceState;
  private SinkRecord record;

  @BeforeEach
  void setUp() {
    sinkTask = new CassandraSinkTask();
    instanceState = mock(InstanceState.class);
    sinkTask.getProcessor().setInstanceState(instanceState);
    record = new SinkRecord("mytopic", 0, null, null, null, "value", 1234L);
  }

  @Test
  void should_map_and_queue_record() {
    // Test that if we have two mappings for one topic, we produce two bound statements.

    @SuppressWarnings("unchecked")
    BlockingQueue<RecordAndStatement> queue = new LinkedBlockingQueue<>();

    // Topic settings, using a LinkedHashMap for deterministic iteration order.
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put("topic.mytopic.ks.mytable.mapping", "c1=value");
    settings.put("topic.mytopic.ks.mytable.consistencyLevel", "ONE");
    settings.put("topic.mytopic.ks.mytable2.mapping", "c2=value");
    settings.put("topic.mytopic.ks.mytable2.consistencyLevel", "QUORUM");

    TopicConfig topicConfig = new TopicConfig("mytopic", settings, false);
    when(instanceState.getTopicConfig("mytopic")).thenReturn(topicConfig);
    List<TableConfig> tableConfigs = new ArrayList<>(topicConfig.getTableConfigs());
    assertThat(tableConfigs.size()).isEqualTo(2);

    RecordMapper recordMapper1 = mock(RecordMapper.class);
    RecordMapper recordMapper2 = mock(RecordMapper.class);
    when(instanceState.getRecordMapper(tableConfigs.get(0))).thenReturn(recordMapper1);
    when(instanceState.getRecordMapper(tableConfigs.get(1))).thenReturn(recordMapper2);
    BoundStatement bs1 = mock(BoundStatement.class);
    BoundStatement bs2 = mock(BoundStatement.class);
    when(recordMapper1.map(any(), any())).thenReturn(bs1);
    when(recordMapper2.map(any(), any())).thenReturn(bs2);
    when(bs1.setConsistencyLevel(any())).thenReturn(bs1);
    when(bs2.setConsistencyLevel(any())).thenReturn(bs2);

    sinkTask.getProcessor().mapAndQueueRecord(queue, new KafkaSinkRecordAdapter(record));
    assertThat(queue.size()).isEqualTo(2);
    assertThat(Objects.requireNonNull(queue.poll()).getStatement()).isSameAs(bs1);
    assertThat(Objects.requireNonNull(queue.poll()).getStatement()).isSameAs(bs2);
    verify(bs1).setConsistencyLevel(DefaultConsistencyLevel.ONE);
    verify(bs2).setConsistencyLevel(DefaultConsistencyLevel.QUORUM);
  }

  @Test
  void should_retry_batch_with_backoff_when_errors_are_not_tolerated() {
    SinkTaskContext context = givenIgnoreErrors(IgnoreErrorsPolicy.NONE);
    sinkTask.handleFailure(record, new RuntimeException("timeout"), "INSERT INTO ks.t");

    assertThatThrownBy(sinkTask::retryBatchIfNeeded).isInstanceOf(RetriableException.class);
    ArgumentCaptor<Long> backoff = ArgumentCaptor.forClass(Long.class);
    verify(context).timeout(backoff.capture());
    assertThat(backoff.getValue()).isBetween(500L, 1_000L);
    verify(context, never()).offset(any(TopicPartition.class), anyLong());
  }

  @Test
  void should_retry_batch_on_mapping_failure_when_errors_are_not_tolerated() {
    givenIgnoreErrors(IgnoreErrorsPolicy.NONE);
    sinkTask.handleFailure(record, new RuntimeException("bad record"), null);

    assertThatThrownBy(sinkTask::retryBatchIfNeeded).isInstanceOf(RetriableException.class);
  }

  @Test
  void should_grow_backoff_up_to_the_cap() {
    SinkTaskContext context = givenIgnoreErrors(IgnoreErrorsPolicy.NONE);
    sinkTask.handleFailure(record, new RuntimeException("timeout"), "INSERT INTO ks.t");
    for (int i = 0; i < 10; i++) {
      assertThatThrownBy(sinkTask::retryBatchIfNeeded).isInstanceOf(RetriableException.class);
    }

    ArgumentCaptor<Long> backoff = ArgumentCaptor.forClass(Long.class);
    verify(context, times(10)).timeout(backoff.capture());
    List<Long> values = backoff.getAllValues();
    assertThat(values.get(1)).isBetween(1_000L, 2_000L);
    assertThat(values.get(2)).isBetween(2_000L, 4_000L);
    assertThat(values).allSatisfy(v -> assertThat(v).isLessThanOrEqualTo(60_000L));
    assertThat(values.get(9)).isBetween(30_000L, 60_000L);
  }

  @Test
  void should_fail_task_once_retries_are_exhausted() {
    givenIgnoreErrors(IgnoreErrorsPolicy.NONE);
    sinkTask.handleFailure(record, new RuntimeException("value=secret"), "INSERT INTO ks.t");
    for (int i = 0; i < CassandraSinkTask.MAX_RETRIES; i++) {
      assertThatThrownBy(sinkTask::retryBatchIfNeeded).isInstanceOf(RetriableException.class);
    }

    assertThatThrownBy(sinkTask::retryBatchIfNeeded)
        .isExactlyInstanceOf(ConnectException.class)
        .hasMessageContaining("after 10 retries")
        .hasMessageContaining(RuntimeException.class.getName())
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain("secret"));
  }

  @Test
  void should_reset_backoff_after_a_clean_batch() {
    SinkTaskContext context = givenIgnoreErrors(IgnoreErrorsPolicy.NONE);
    sinkTask.handleFailure(record, new RuntimeException("timeout"), "INSERT INTO ks.t");
    for (int i = 0; i < 3; i++) {
      assertThatThrownBy(sinkTask::retryBatchIfNeeded).isInstanceOf(RetriableException.class);
    }

    sinkTask.put(Collections.emptyList());
    sinkTask.handleFailure(record, new RuntimeException("timeout"), "INSERT INTO ks.t");
    assertThatThrownBy(sinkTask::retryBatchIfNeeded).isInstanceOf(RetriableException.class);

    ArgumentCaptor<Long> backoff = ArgumentCaptor.forClass(Long.class);
    verify(context, times(4)).timeout(backoff.capture());
    assertThat(backoff.getAllValues().get(3)).isBetween(500L, 1_000L);
  }

  @Test
  void should_not_retry_batch_when_errors_are_tolerated() {
    SinkTaskContext context = givenIgnoreErrors(IgnoreErrorsPolicy.ALL);
    sinkTask.handleFailure(record, new RuntimeException("timeout"), "INSERT INTO ks.t");

    sinkTask.retryBatchIfNeeded();
    verify(context, never()).timeout(anyLong());
    verify(context, never()).offset(any(TopicPartition.class), anyLong());
  }

  private SinkTaskContext givenIgnoreErrors(IgnoreErrorsPolicy policy) {
    CassandraSinkConfig config = mock(CassandraSinkConfig.class);
    when(config.getIgnoreErrors()).thenReturn(policy);
    when(instanceState.getConfig()).thenReturn(config);
    SinkTaskContext context = mock(SinkTaskContext.class);
    sinkTask.initialize(context);
    return context;
  }
}
