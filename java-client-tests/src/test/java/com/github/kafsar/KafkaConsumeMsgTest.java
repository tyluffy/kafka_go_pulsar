/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.kafsar;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KafkaConsumeMsgTest extends BaseTest {

    @BeforeAll
    public void beforeAll() throws PulsarClientException {
        super.init();
    }

    @Test
    @Timeout(60)
    public void consumeEarliestMsgSuccess() throws Exception {
        String topic = UUID.randomUUID().toString();
        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING).enableBatching(false);
        Producer<String> producer = producerBuilder.topic(topic).create();
        String msg = UUID.randomUUID().toString();
        producer.send(msg + "1");
        producer.send(msg + "2");
        TimeUnit.SECONDS.sleep(10);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_EARLIER);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() == 0) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
            Assertions.assertEquals(msg + "1", consumerRecord.value());
            break;
        }
    }

    @Test
    @Timeout(60)
    public void consumeLatestMsgSuccess() throws Exception {
        String topic = UUID.randomUUID().toString();
        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING).enableBatching(false);
        Producer<String> producer = producerBuilder.topic(topic).create();
        String msg = UUID.randomUUID().toString();
        producer.send(msg + "1");
        producer.send(msg + "2");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_LATEST);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        TimeUnit.SECONDS.sleep(5);
        CompletableFuture<MessageId> completableFuture = producer.sendAsync(msg + "3");
        TimeUnit.SECONDS.sleep(5);
        while (true) {
            if (completableFuture.isCompletedExceptionally()) {
                throw new Exception("send pulsar message 3 failed");
            }
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() == 0) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
            log.info("record value is {} offset is {}", consumerRecord.value(), consumerRecord.offset());
            Assertions.assertEquals(msg + "3", consumerRecord.value());
            break;
        }
    }

    @Test
    @Timeout(60)
    public void retryConsumeLatestMsgAfterClose() throws Exception {
        String topic = UUID.randomUUID().toString();
        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING).enableBatching(false);
        Producer<String> producer = producerBuilder.topic(topic + "-partition-0").create();
        String msg = UUID.randomUUID().toString();
        producer.send(msg + "1");
        producer.send(msg + "2");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_LATEST);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        TimeUnit.SECONDS.sleep(5);
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() == 0) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
            log.info("record value is {} offset is {}", consumerRecord.value(), consumerRecord.offset());
            Assertions.assertEquals(msg + "2", consumerRecord.value());
            consumer.commitSync();
            break;
        }
        consumer.close();
        TimeUnit.SECONDS.sleep(5);
        CompletableFuture<MessageId> completableFutureRetry = producer.sendAsync(msg + "4");
        Consumer<String, String> consumerRetry = new KafkaConsumer<>(props);
        consumerRetry.subscribe(Collections.singletonList(topic));
        TimeUnit.SECONDS.sleep(5);
        while (true) {
            if (completableFutureRetry.isCompletedExceptionally()) {
                throw new Exception("send pulsar message 4 failed");
            }
            ConsumerRecords<String, String> consumerRecords = consumerRetry.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() == 0) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
            log.info("record value is {} offset is {}", consumerRecord.value(), consumerRecord.offset());
            Assertions.assertEquals(msg + "4", consumerRecord.value());
            break;
        }
    }


    @Test
    @Timeout(60)
    public void retryConsumeEarliestMsgAfterClose() throws Exception {
        String topic = UUID.randomUUID().toString();
        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING).enableBatching(false);
        Producer<String> producer = producerBuilder.topic(topic + "-partition-0").create();
        String msg = UUID.randomUUID().toString();
        producer.send(msg + "1");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_EARLIER);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        TimeUnit.SECONDS.sleep(5);
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() == 0) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
            log.info("record value is {} offset is {}", consumerRecord.value(), consumerRecord.offset());
            Assertions.assertEquals(msg + "1", consumerRecord.value());
            consumer.commitSync();
            TimeUnit.MILLISECONDS.sleep(200);
            break;
        }
        consumer.close();
        TimeUnit.SECONDS.sleep(5);
        CompletableFuture<MessageId> completableFutureRetry = producer.sendAsync(msg + "4");
        Consumer<String, String> consumerRetry = new KafkaConsumer<>(props);
        consumerRetry.subscribe(Collections.singletonList(topic));
        TimeUnit.SECONDS.sleep(5);
        while (true) {
            if (completableFutureRetry.isCompletedExceptionally()) {
                throw new Exception("send pulsar message 4 failed");
            }
            ConsumerRecords<String, String> consumerRecords = consumerRetry.poll(Duration.ofSeconds(1));
            if (consumerRecords.count() == 0) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            ConsumerRecord<String, String> consumerRecord = consumerRecords.iterator().next();
            log.info("record value is {} offset is {}", consumerRecord.value(), consumerRecord.offset());
            Assertions.assertEquals(msg + "4", consumerRecord.value());
            break;
        }
    }

    @Test
    @Timeout(60)
    public void multiConsumerTest() throws Exception {
        String topic = UUID.randomUUID().toString();
        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING).enableBatching(false);
        Producer<String> producer = producerBuilder.topic(topic).create();
        String msg = UUID.randomUUID().toString();
        producer.send(msg);
        producer.send(msg);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_LATEST);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Consumer<String, String> consumer1 = new KafkaConsumer<>(props);
        Consumer<String, String> consumer2 = new KafkaConsumer<>(props);

        new Thread(() -> consumer1.subscribe(Collections.singletonList(topic))).start();
        new Thread(() -> consumer2.subscribe(Collections.singletonList(topic))).start();

        TimeUnit.SECONDS.sleep(1);

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer2.poll(Duration.ofSeconds(1));
            if (consumerRecords.isEmpty()) {
                TimeUnit.MILLISECONDS.sleep(200);
            } else {
                Assertions.assertEquals(msg, consumerRecords.iterator().next().value());
                break;
            }
        }
    }


    @Test
    @Timeout(60)
    public void ConsumeEarliestWithNoMsgTest() throws Exception {
        String topic = UUID.randomUUID().toString();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_EARLIER);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Consumer<String, String> consumer1 = new KafkaConsumer<>(props);
        consumer1.subscribe(Collections.singletonList(topic));

        TimeUnit.SECONDS.sleep(1);

        for (int i = 0; i < 3; i++) {
            ConsumerRecords<String, String> consumerRecords = consumer1.poll(Duration.ofSeconds(1));
            Assertions.assertTrue(consumerRecords.isEmpty());
        }
    }

    @Test
    @Timeout(60)
    public void ConsumeLatestWithNoMsgTest() throws Exception {
        String topic = UUID.randomUUID().toString();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_LATEST);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);

        Consumer<String, String> consumer1 = new KafkaConsumer<>(props);
        consumer1.subscribe(Collections.singletonList(topic));

        TimeUnit.SECONDS.sleep(1);

        for (int i = 0; i < 3; i++) {
            ConsumerRecords<String, String> consumerRecords = consumer1.poll(Duration.ofSeconds(1));
            Assertions.assertTrue(consumerRecords.isEmpty());
        }
    }

    @Test
    @Timeout(60)
    public void ConsumeMsgWithMultiPartition() throws Exception {
        String topic = UUID.randomUUID().toString();
        pulsarAdmin.topics().createPartitionedTopic("public/default/" + topic, 3);
        ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING).enableBatching(false);
        Producer<String> producer = producerBuilder.topic(topic).create();
        String msg = UUID.randomUUID().toString();
        producer.send(msg + "0");
        producer.send(msg + "1");
        producer.send(msg + "2");
        producer.send(msg + "3");
        producer.send(msg + "4");
        producer.send(msg + "5");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConst.BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConst.OFFSET_RESET_EARLIER);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String saslJaasConfig = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required \nusername=\"%s\" \npassword=\"%s\";", "alice", "pwd");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        Consumer<String, String> consumer1 = new KafkaConsumer<>(props);
        consumer1.subscribe(Collections.singletonList(topic));
        TimeUnit.SECONDS.sleep(1);

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer1.poll(Duration.ofSeconds(1));
            if (consumerRecords.isEmpty()) {
                continue;
            }
            Assertions.assertEquals(6, consumerRecords.count());
            break;
        }
    }

    @AfterAll
    public void afterAll() throws PulsarClientException {
        super.close();
    }

}
