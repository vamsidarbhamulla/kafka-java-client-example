package com.bodhivruksha.demo.kafka;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
@Getter
@AllArgsConstructor
public class KafkaClientRecord<K, V> {
  private final K key;
  private final V message;

  private RecordMetadata recordMetadata;

  public KafkaClientRecord(K key, V message) {
    this(key, message, null);
  }

  public void setRecordMetadata(RecordMetadata recordMetadata) {
    this.recordMetadata = recordMetadata;
  }
}
