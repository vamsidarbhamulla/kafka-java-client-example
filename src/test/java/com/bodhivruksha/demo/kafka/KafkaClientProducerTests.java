package com.bodhivruksha.demo.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

public class KafkaClientProducerTests {

  private static final String TOPIC = "TEST_TOPIC";
  private MockProducer<Long, String> mockProducer = new MockProducer<>();

  @Test
  void producer_records_count_matched() {
    var messages = List.of("Message_0", "Message_1", "Message_2");
    var clientProducer = producer(messages);

    clientProducer.sendWithAsyncMetadata();
    List<ProducerRecord<Long, String>> history = mockProducer.history();

    List<ProducerRecord<Long, String>> expected =
        Arrays.asList(
            new ProducerRecord<>(TOPIC, 0L, "Message_0"),
            new ProducerRecord<>(TOPIC, 1L, "Message_1"),
            new ProducerRecord<>(TOPIC, 2L, "Message_2"));

    SoftAssertions softly = new SoftAssertions();
    softly.assertThat(history.get(0).topic()).isEqualTo(expected.get(0).topic());
    softly.assertThat(history.get(0).key()).isNotNull();
    softly.assertThat(history.get(0).value()).isEqualTo(expected.get(0).value());
    softly.assertThat(history.get(1).topic()).isEqualTo(expected.get(1).topic());
    softly.assertThat(history.get(1).key()).isNotNull();
    softly.assertThat(history.get(1).value()).isEqualTo(expected.get(1).value());
    softly.assertThat(history.get(2).topic()).isEqualTo(expected.get(2).topic());
    softly.assertThat(history.get(2).key()).isNotNull();
    softly.assertThat(history.get(2).value()).isEqualTo(expected.get(2).value());
    softly.assertAll();
  }

  @Test
  void producer_record_list_size_matched() {
    var messages =
        List.of(
            "Message_0",
            "Message_1",
            "Message_2",
            "Message_3",
            "Message_4",
            "Message_5",
            "Message_6",
            "Message_7",
            "Message_8",
            "Message_9");
    var clientProducer = producer(messages);
    var recordList = clientProducer.sendWithAsyncMetadata();

    assertEquals(10, recordList.size(), "Record list size is not ten");
  }

  @Test
  void producer_latch_count_reduced() {
    var latch = new CountDownLatch(1);
    var messages = List.of(buildMessage().get(), buildMessage().get());
    var clientProducer = new KafkaClientProducer<>(mockProducer, latch, TOPIC, null, messages);
    clientProducer.call();

    assertEquals(0, latch.getCount(), "Countdown latch count not reduced");
  }

  @Test
  void producer_record_metadata_exists_when_send_synchronously() {
    var messages = List.of(buildMessage().get(), buildMessage().get());
    var clientProducer = producer(messages);
    var recordList = clientProducer.sendWithSyncMetadata();

    var recordMetadata1 = recordList.get(0).getRecordMetadata();
    var recordMetadata2 = recordList.get(1).getRecordMetadata();

    SoftAssertions softly = new SoftAssertions();
    softly.assertThat(recordMetadata1.topic()).isEqualTo(TOPIC);
    softly.assertThat(recordMetadata1.partition()).isEqualTo(0);
    softly.assertThat(recordMetadata1.offset()).isEqualTo(0);
    softly.assertThat(recordMetadata1.toString()).isEqualTo("TEST_TOPIC-0@0");

    softly.assertThat(recordMetadata2.topic()).isEqualTo(TOPIC);
    softly.assertThat(recordMetadata2.partition()).isEqualTo(0);
    softly.assertThat(recordMetadata2.offset()).isEqualTo(1);
    softly.assertThat(recordMetadata2.toString()).isEqualTo("TEST_TOPIC-0@1");

    softly.assertAll();
  }

  private KafkaClientProducer<Long, String> producer(List<String> messages) {
    return new KafkaClientProducer<>(mockProducer, TOPIC, messages);
  }

  private Supplier<String> buildMessage() {
    var token = "12345";
    return () ->
        "{\n"
            + "  \"events\": [{\n"
            + "    \"eventVersion\": \"0.0\",\n"
            + "    \"eventSource\": \"dispatcher\",\n"
            + "    \"eventTime\": \""
            + System.currentTimeMillis()
            + " \",\n"
            + "    \"eventName\": \"Bid\",\n"
            + "    \"correlationId\": \""
            + UUID.randomUUID().toString()
            + "\",\n"
            + "    \"userIdentity\": {\n"
            + "      \"principalId\": \"requester-1\"\n"
            + "    },\n"
            + "    \"requestParameters\": {\n"
            + "      \"auth\": \"bearer \" "
            + token
            + "\n"
            + "    },\n"
            + "    \"dispatcher\": {\n"
            + "      \"schemaVersion\": \"12\",\n"
            + "      \"content\": \"1234\"\n"
            + "    }\n"
            + "  }]\n"
            + "}";
  }
}
