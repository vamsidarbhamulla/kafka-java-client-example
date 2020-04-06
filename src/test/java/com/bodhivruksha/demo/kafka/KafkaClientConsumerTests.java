package com.bodhivruksha.demo.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

@Slf4j
public class KafkaClientConsumerTests {

  private static final String TOPIC = "TEST_TOPIC";
  private MockConsumer<String, String> mockConsumer =
      new MockConsumer<>(OffsetResetStrategy.EARLIEST);

  @Test
  void test_assign_consumer() {
    var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
    mockConsumer.assign(List.of(new TopicPartition(TOPIC, 0)));

    var clientRecord = new KafkaClientRecord<>("key_0", "Message_0");
    var consumerClient = new KafkaClientConsumer<>(mockConsumer, TOPIC);

    Set<TopicPartition> partitions = mockConsumer.assignment();
    partitions.forEach(part -> log.info("Assigned Partitions: {}", part.partition()));

    // Mock consumers need to seek manually since they cannot automatically reset offsets
    Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
    beginningOffsets.put(new TopicPartition(TOPIC, 0), 0L);
    beginningOffsets.put(new TopicPartition(TOPIC, 1), 0L);
    mockConsumer.updateBeginningOffsets(beginningOffsets);
    mockConsumer.seek(new TopicPartition(TOPIC, 0), 0);
    ConsumerRecord<String, String> rec1 =
        new ConsumerRecord<>(
            TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key_0", "Message_0");
    ConsumerRecord<String, String> rec2 =
        new ConsumerRecord<>(
            TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key_2", "Message_1");
    mockConsumer.addRecord(rec1);
    mockConsumer.addRecord(rec2);

    consumerClient.fetchRecords();
  }

  @Test
  void test_subscribe_consumer() {
    mockConsumer.subscribe(List.of(TOPIC), new NoOpConsumerRebalanceListener());
    Set<TopicPartition> partitions = mockConsumer.assignment();
    partitions.forEach(part -> log.info("Assigned Partitions: {}", part.partition()));

    mockConsumer.rebalance(List.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1)));

    // Mock consumers need to seek manually since they cannot automatically reset offsets
    Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
    beginningOffsets.put(new TopicPartition(TOPIC, 0), 0L);
    beginningOffsets.put(new TopicPartition(TOPIC, 1), 0L);
    mockConsumer.updateBeginningOffsets(beginningOffsets);
    mockConsumer.seek(new TopicPartition(TOPIC, 0), 0);
    ConsumerRecord<String, String> rec1 =
        new ConsumerRecord<>(
            TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key1", "value1");
    ConsumerRecord<String, String> rec2 =
        new ConsumerRecord<>(
            TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key2", "value2");
    mockConsumer.addRecord(rec1);
    mockConsumer.addRecord(rec2);

    var clientRecord1 = new KafkaClientRecord<>("key1", "value1");
    var clientRecord2 = new KafkaClientRecord<>("key2", "value2");
    var consumerClient = new KafkaClientConsumer<>(mockConsumer, TOPIC);
    consumerClient.fetchRecords();
  }
}
