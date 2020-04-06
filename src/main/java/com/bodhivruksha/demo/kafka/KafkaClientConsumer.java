package com.bodhivruksha.demo.kafka;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class KafkaClientConsumer<K, V> implements Callable<List<KafkaClientRecord<K, V>>> {

  private final CountDownLatch latch;
  private final Consumer<K, V> consumer;
  private final String topic;
  private final Integer partition;
  private Long position;

  public KafkaClientConsumer(
      Consumer<K, V> consumer,
      CountDownLatch latch,
      String topic,
      Integer partition,
      Long position) {
    this.consumer = consumer;
    this.position = position;
    this.latch = latch;
    this.topic = topic;
    this.partition = partition;
  }

  public KafkaClientConsumer(Consumer<K, V> consumer, CountDownLatch latch, String topic) {
    this(consumer, latch, topic, 0, 0L);
  }

  public KafkaClientConsumer(
      Consumer<K, V> consumer, CountDownLatch latch, String topic, Long position) {
    this(consumer, latch, topic, 0, position);
  }

  public KafkaClientConsumer(Consumer<K, V> consumer, String topic) {
    this(consumer, null, topic, 0, 0L);
  }

  @Override
  public List<KafkaClientRecord<K, V>> call() {
    subscribe();
    var consumerRecords = fetchRecords();
    consumer.unsubscribe();

    if (latch != null) {
      latch.countDown();
    }

    consumer.close();
    return consumerRecords;
  }

  public void subscribe() {
    final Map<TopicPartition, Long> offsets =
        Map.of(new TopicPartition(topic, partition), position);
    consumer.subscribe(
        List.of(topic),
        new ConsumerRebalanceListener() {

          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("Assigned {}", partitions);

            var partitionsSet = new HashSet<>(partitions);
            Map<TopicPartition, OffsetAndMetadata> metadata = consumer.committed(partitionsSet);

            metadata.forEach(
                (topicPartition, offsetAndMetadata) -> {
                  if (offsetAndMetadata != null) {
                    log.info("Current offset is {}", offsetAndMetadata.offset());
                  } else {
                    log.info("No committed offsets");
                  }
                  Long offset = offsets.get(topicPartition);
                  if (offset != null) {
                    log.info("Seeking to {} ", offset);
                    consumer.seek(topicPartition, offset);
                  }
                });
          }
        });
  }

  /* public List<KafkaClientRecord<K, V>> assign(Long position) {
    var topicPartition = new TopicPartition(topic, partition);
    consumer.seek(topicPartition, position);
    return fetchRecords();
  }

  public void assign() {
    assign(position());
  }*/

  List<KafkaClientRecord<K, V>> fetchRecords() {
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1000));
    List<KafkaClientRecord<K, V>> fetchedRecords = new ArrayList<>();

    for (TopicPartition partition : records.partitions()) {
      List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
      for (ConsumerRecord<K, V> record : partitionRecords) {

        log.info(
            "Received message at time {} : ({}: {}) in partition {} at offset {}",
            record.timestamp(),
            record.key(),
            record.value(),
            record.partition(),
            record.offset());

        var recordCheck = new KafkaClientRecord<>(record.key(), record.value());
        fetchedRecords.add(recordCheck);
      }
      /* long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
      consumer.commitSync(Map.of(partition, new OffsetAndMetadata(lastOffset + 1)));
      return fetchedRecords; */
    }

    return fetchedRecords;
  }

  public Long position() {
    var topicPartition = new TopicPartition(topic, partition);
    consumer.assign(List.of(topicPartition));
    consumer.seekToEnd(List.of(topicPartition));
    return consumer.position(topicPartition, Duration.ofSeconds(1000L));
  }
}
