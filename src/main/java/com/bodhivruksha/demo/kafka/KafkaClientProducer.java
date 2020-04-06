package com.bodhivruksha.demo.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class KafkaClientProducer<K, V> implements Callable<List<KafkaClientRecord<K, V>>> {

  private final Producer<K, V> producer;
  private final CountDownLatch latch;
  private final String topic;
  private final Integer partition;
  private final List<String> messages;

  private List<KafkaClientRecord<K, V>> kafkaClientRecordList = new ArrayList<>();

  public KafkaClientProducer(
      Producer<K, V> producer,
      CountDownLatch latch,
      String topic,
      Integer partition,
      List<String> messages) {
    this.producer = producer;
    this.latch = latch;
    this.topic = topic;
    this.partition = partition;
    this.messages = messages;
  }

  public KafkaClientProducer(Producer<K, V> producer, String topic, List<String> messages) {
    this(producer, null, topic, 0, messages);
  }

  @Override
  public List<KafkaClientRecord<K, V>> call() {
    var kafkaClientRecords = sendWithAsyncMetadata();
    //    sendWithSyncMetadata();

    if (latch != null) {
      latch.countDown();
    }

    kafkaClientRecordList = kafkaClientRecords;
    return kafkaClientRecords;
  }

  public List<KafkaClientRecord<K, V>> sendWithAsyncMetadata() {
    var kafkaClientRecordList = new ArrayList<KafkaClientRecord<K, V>>();
    for (String messageStr : messages) {
      var startTime = System.currentTimeMillis();

      var recordCheck = new KafkaClientRecord(startTime, messageStr);

      producer.send(
          new ProducerRecord(topic, startTime, messageStr),
          new ProducerCallBack<K, V>(startTime, recordCheck));

      log.info("Sent message: ( {} , {} )", startTime, messageStr);

      kafkaClientRecordList.add(recordCheck);
    }

    return kafkaClientRecordList;
  }

  public List<KafkaClientRecord<K, V>> sendWithSyncMetadata() {
    List<KafkaClientRecord<K, V>> kafkaClientRecordList = new ArrayList<>();
    for (int messageNo = 0; messageNo < messages.size(); messageNo++) {
      var messageStr = messages.get(messageNo);
      var startTime = System.currentTimeMillis();
      var recordCheck = new KafkaClientRecord(startTime, messageStr);
      try {
        final Future<RecordMetadata> future =
            producer.send(new ProducerRecord(topic, startTime, messageStr));

        log.info("Sent message: ({} , {})", startTime, messageStr);

        producer.flush();
        while (!future.isDone()) {
          Thread.sleep(10L);
        }

        RecordMetadata recordMetadata = future.get();
        log.info("Produce completed");

        log.info(
            "message({}, {}) sent to partition({}), offset({})",
            messageNo,
            messageStr,
            recordMetadata.partition(),
            recordMetadata.offset());

        recordCheck.setRecordMetadata(recordMetadata);
        kafkaClientRecordList.add(recordCheck);

      } catch (InterruptedException | ExecutionException e) {
        log.error("Error when sending with synced metadata {}", e.getClass().getName());
      }
    }
    return kafkaClientRecordList;
  }

  public void close() {
    producer.close();
  }

  public List<KafkaClientRecord<K, V>> getRecordCheckList() {
    return kafkaClientRecordList;
  }
}

@Slf4j
class ProducerCallBack<K, V> implements Callback {

  private final long startTime;
  private final KafkaClientRecord<K, V> kafkaClientRecord;

  ProducerCallBack(long startTime, KafkaClientRecord<K, V> kafkaClientRecord) {
    this.startTime = startTime;
    this.kafkaClientRecord = kafkaClientRecord;
  }

  /**
   * A callback method the user can implement to provide asynchronous handling of request
   * completion. This method will be called when the record sent to the server has been
   * acknowledged. When exception is not null in the callback, metadata will contain the special -1
   * value for all fields except for topicPartition, which will be valid.
   *
   * @param recordMetadata The metadata for the record that was sent (i.e. the partition and
   *     offset). Null if an error occurred.
   * @param exception The exception thrown during processing of this record. Null if no error
   *     occurred.
   */
  @Override
  public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
    long elapsedTime = System.currentTimeMillis() - startTime;
    if (recordMetadata != null) {
      kafkaClientRecord.setRecordMetadata(recordMetadata);
      log.info(
          "Producer Call back message ({}, {}) sent to partition({}), offset({}) in {} ms",
          kafkaClientRecord.getKey(),
          kafkaClientRecord.getMessage(),
          recordMetadata.partition(),
          recordMetadata.offset(),
          elapsedTime);
    } else {
      log.error(String.valueOf(exception));
    }
  }
}
