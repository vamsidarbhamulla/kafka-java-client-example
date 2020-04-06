package com.bodhivruksha.demo.kafka;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.*;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaITests {

  private static final String TOPIC = "test-stream";
  private static final String KAFKA_SERVICE_NAME = "kafka_1";
  private static final Integer KAFKA_SERVER_PORT = 9092;

  /*DockerComposeContainer environment =
  new DockerComposeContainer<>(new File("tools/docker-compose.yml"))
          .withExposedService(
                  KAFKA_SERVICE_NAME,
                  KAFKA_SERVER_PORT,
                  Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)))
          .withLogConsumer(KAFKA_SERVICE_NAME, new Slf4jLogConsumer(log))
          .withLocalCompose(true)
          .withEnv("DOCKERHOST", dockerHostIpAddr());*/

  @BeforeAll
  void setup() {
    //    environment.start();
  }

  @AfterAll
  void tearDown() {}

  @Test
  @DisplayName("Kafka integration test with parallel producer and consumer")
  void kafka_infra_integration_test_parallel() {
    var executor = Executors.newFixedThreadPool(2);
    var latch = new CountDownLatch(2);

    var consumerRunnable = buildConsumer(latch, 0L);
    var offset = consumerRunnable.position();
    consumerRunnable = buildConsumer(latch, offset);

    var consumerFuture = executor.submit(consumerRunnable);

    var producerRunnable = buildProducer(latch);
    var producerFuture = executor.submit(producerRunnable);

    try {
      latch.await();

      waitForPublish(producerFuture);

      var consumerRecords = consumerFuture.get(1, TimeUnit.MINUTES);
      var producerRecords = producerFuture.get(1, TimeUnit.MINUTES);
      check(consumerRecords, producerRecords);

      producerRunnable.close();

      log.info("attempting to shutdown executor");
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.warn("tasks interrupted due to an error: {}", e.getClass().getName());
    } finally {
      if (!executor.isTerminated()) {
        log.warn("cancel non-finished tasks");
      }
      executor.shutdownNow();
      log.info("shutdown finished");
    }
  }

  private KafkaClientProducer<Long, String> buildProducer(CountDownLatch latch) {
    return new KafkaClientProducer<>(producer.get(), latch, TOPIC, null, messages());
  }

  private void waitForPublish(Future<List<KafkaClientRecord<Long, String>>> producerFuture) {
    await()
        .alias("wait for kafka producer")
        .pollInterval(Duration.ofMillis(100))
        .atMost(Duration.ofMinutes(1))
        .until(
            () ->
                producerFuture.get(1, TimeUnit.MILLISECONDS) != null
                    && producerFuture.get(1, TimeUnit.MICROSECONDS).get(0).getRecordMetadata()
                        != null);
  }

  private KafkaClientConsumer<Long, String> buildConsumer(CountDownLatch latch, Long position) {
    return new KafkaClientConsumer<>(consumer.get(), latch, TOPIC, position);
  }

  private Supplier<Producer<Long, String>> producer =
      () -> {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-commons-it-producer" + UUID.randomUUID());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
      };

  private Supplier<KafkaConsumer<Long, String>> consumer =
      () -> {
        var props = new Properties();

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        props.put(
            ConsumerConfig.GROUP_ID_CONFIG, "test-commons-it-consumer-group" + UUID.randomUUID());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
      };

  private String bootStrapServer() {
    return dockerHostIpAddr() + ":" + KAFKA_SERVER_PORT;
  }

  private String dockerHostIpAddr() {
    var command =
        "ifconfig | grep -E \"([0-9]{1,3}\\.){3}[0-9]{1,3}\" | grep -v 127.0.0.1 | awk '{ print $2 }' | cut -f2 -d: | head -n1";
    var commandResult = new StringBuilder();
    try {
      var processBuilder = new ProcessBuilder();
      processBuilder.redirectErrorStream(true);
      processBuilder.command("/bin/bash", "-c", command);
      var process = processBuilder.start();
      var bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      String bufferedReaderLine;
      while ((bufferedReaderLine = bufferedReader.readLine()) != null) {
        commandResult.append(bufferedReaderLine);
      }
      process.waitFor();
      log.info("\n \" {} \" command executed with status: {}", command, process.exitValue());
      process.destroy();
    } catch (IOException | InterruptedException e) {
      log.error("Error while executing the command {}", e.getClass().getName());
    }

    return commandResult.toString();
  }

  private List<String> messages() {
    var date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
    return List.of(
        "kafka_itest_message_0: " + date,
        "kafka_itest_message_1: " + date,
        "kafka_itest_message_2: " + date,
        "kafka_itest_message_3: " + date,
        "kafka_itest_message_4: " + date,
        "kafka_itest_message_5: " + date,
        "kafka_itest_message_6: " + date,
        "kafka_itest_message_7: " + date,
        "kafka_itest_message_8: " + date,
        "kafka_itest_message_9: " + date);
  }

  void check(
      List<KafkaClientRecord<Long, String>> consumerRecords,
      List<KafkaClientRecord<Long, String>> producerRecords) {

    if (consumerRecords == null
        || producerRecords == null
        || consumerRecords.size() == 0
        || producerRecords.size() == 0) {
      fail("Consumer records or producer records are empty");
    }

    SoftAssertions softly = new SoftAssertions();
    consumerRecords.sort(Comparator.comparing(KafkaClientRecord::getKey));
    producerRecords.sort(Comparator.comparing(KafkaClientRecord::getKey));

    for (int i = 0; i < consumerRecords.size(); i++) {
      var consumerRecord = consumerRecords.get(i);
      var producerRecord = producerRecords.get(i);
      if (consumerRecord.getKey().equals(producerRecord.getKey())) {
        checkOne(consumerRecord, producerRecord, softly);
      }
    }

    softly.assertAll();
  }

  public void checkOne(
      KafkaClientRecord<Long, String> consumerRecord,
      KafkaClientRecord<Long, String> producerRecord,
      SoftAssertions softly) {
    softly.assertThat(consumerRecord.getKey()).isEqualTo(producerRecord.getKey());
    softly.assertThat(consumerRecord.getMessage()).isEqualTo(producerRecord.getMessage());
  }
}
