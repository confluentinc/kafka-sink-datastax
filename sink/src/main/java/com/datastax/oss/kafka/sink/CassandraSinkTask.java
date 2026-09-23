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

import com.datastax.oss.common.sink.AbstractSinkRecord;
import com.datastax.oss.common.sink.AbstractSinkTask;
import com.datastax.oss.common.sink.config.CassandraSinkConfig.IgnoreErrorsPolicy;
import com.datastax.oss.common.sink.state.InstanceState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CassandraSinkTask does the heavy lifting of processing {@link SinkRecord}s and writing them to
 * DSE.
 */
public class CassandraSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(CassandraSinkTask.class);
  public static final String KAFKA_CONNECTOR_APPLICATION_NAME = "DataStax Apache Kafka Connector";

  private Map<TopicPartition, OffsetAndMetadata> failureOffsets;
  private final AbstractSinkTask processor = new SinkTaskProcessorImpl();

  // KIP-610 errant record reporter, used to route tolerated write/mapping failures to the DLQ
  private ErrantRecordReporter reporter;
  private final Queue<Future<Void>> reportFutures = new ConcurrentLinkedQueue<>();

  @Override
  public String version() {
    return processor.version();
  }

  @Override
  public void start(Map<String, String> props) {
    log.debug("CassandraSinkTask starting with props: {}", props);
    failureOffsets = new ConcurrentHashMap<>();
    try {
      reporter = context.errantRecordReporter();
    } catch (NoSuchMethodError | NoClassDefFoundError e) {
      reporter = null;
    }
    processor.start(props);
  }

  /**
   * Invoked by the Connect infrastructure prior to committing offsets to Kafka, which is typically
   * 10 seconds. This is the task's opportunity to report failed record offsets and keeping the sink
   * from progressing on a particular topic.
   *
   * @param currentOffsets map of offsets (one offset for each topic)
   * @return the map, mutated to have failure offsets recorded in it
   */
  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // Ensure any records reported to the dead-letter queue during this cycle are durable before
    // their offsets are allowed to advance past them.
    Future<Void> reportFuture;
    while ((reportFuture = reportFutures.poll()) != null) {
      try {
        reportFuture.get();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while flushing dead-letter-queue reports", ie);
      } catch (Exception ex) {
        log.warn(
            "Failed to confirm a dead-letter-queue report; the record may not have been written "
                + "to the DLQ",
            ex);
      }
    }
    // Copy all of the failures (which point to the offset that we should retrieve from next time)
    // into currentOffsets.
    currentOffsets.putAll(failureOffsets);
    return currentOffsets;
  }

  /**
   * Entry point for record processing.
   *
   * @param sinkRecords collection of Kafka {@link SinkRecord}'s to process
   */
  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    List<AbstractSinkRecord> wrapped = new ArrayList<>(sinkRecords.size());
    sinkRecords.forEach(
        r -> {
          wrapped.add(new KafkaSinkRecordAdapter(r));
        });
    processor.put(wrapped);
  }

  @Override
  public void stop() {
    processor.stop();
  }

  public AbstractSinkTask getProcessor() {
    return processor;
  }

  public InstanceState getInstanceState() {
    return processor.getInstanceState();
  }

  private class SinkTaskProcessorImpl extends AbstractSinkTask {

    /**
     * Handle a failed record.
     *
     * @param abstractRecord the {@link SinkRecord} that failed to process
     * @param e the exception
     * @param cql the cql statement that failed to execute
     * @param failCounter the metric that keeps track of number of failures encountered
     */
    @Override
    protected void handleFailure(
        AbstractSinkRecord abstractRecord, Throwable e, String cql, Runnable failCounter) {
      // Store the topic-partition and offset that had an error. However, we want
      // to keep track of the *lowest* offset in a topic-partition that failed. Because
      // requests are sent in parallel and response ordering is non-deterministic,
      // it's possible for a failure in an insert with a higher offset be detected
      // before that of a lower offset. Thus, we only record a failure if
      // 1. There is no entry for this topic-partition, or
      // 2. There is an entry, but its offset is > our offset.
      //
      // This can happen in multiple invocations of this callback concurrently, so
      // we perform these checks/updates in a synchronized block. Presumably failures
      // don't occur that often, so we don't have to be very fancy here.
      SinkRecord record = ((KafkaSinkRecordAdapter) abstractRecord).getRecord();
      IgnoreErrorsPolicy ignoreErrors = processor.getInstanceState().getConfig().getIgnoreErrors();
      boolean driverFailure = cql != null;
      // Whether this failure must halt progress (stall and retry) rather than being tolerated.
      boolean stall =
          ignoreErrors == IgnoreErrorsPolicy.NONE
              || (ignoreErrors == IgnoreErrorsPolicy.DRIVER && !driverFailure);
      if (stall) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        long currentOffset = Long.MAX_VALUE;
        if (failureOffsets.containsKey(topicPartition)) {
          currentOffset = failureOffsets.get(topicPartition).offset();
        }
        if (record.kafkaOffset() < currentOffset) {
          failureOffsets.put(topicPartition, new OffsetAndMetadata(record.kafkaOffset()));
          context.offset(topicPartition, record.kafkaOffset());
        }
      } else if (reporter != null) {
        // Route the record to the DLQ and let its offset advance
        reportFutures.add(reporter.report(record, e));
      }

      failCounter.run();

      if (driverFailure) {
        log.warn(
            "Error inserting/updating row for Kafka record from topic {} partition {} offset {}: "
                + "{}\n   statement: {}",
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset(),
            e.getClass().getName(),
            cql);
      } else {
        log.warn(
            "Error decoding/mapping Kafka record from topic {} partition {} offset {}: {}",
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset(),
            e.getClass().getName());
      }
    }

    @Override
    public String version() {
      return new CassandraSinkConnector().version();
    }

    @Override
    public String applicationName() {
      return KAFKA_CONNECTOR_APPLICATION_NAME;
    }

    @Override
    protected void beforeProcessingBatch() {
      super.beforeProcessingBatch();
      failureOffsets.clear();
    }

    @Override
    public void start(Map<String, String> props) {
      failureOffsets = new ConcurrentHashMap<>();
      super.start(props);
    }
  }
}
