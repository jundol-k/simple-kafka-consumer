package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;


public class SimpleConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-linux:9092";
    private final static String GROUP_ID = "test-group";

    private static KafkaConsumer<String, String> consumer = null;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutDownThread());
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 명시적 오프셋 커밋을 하겠다는 설정

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new ReblanceListener());

        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("==> {}", record);
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1 , null));
                    consumer.commitSync(currentOffsets);
                }
            }
        } catch (WakeupException e) {
            logger.warn("Wakeup consumer");
        } finally {
            consumer.close();
        }
    }

    public static class ReblanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are assigned");
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are revoked");
            consumer.commitSync(currentOffsets);
        }

    }

    private static class ShutDownThread extends Thread {
        public void run() {
            logger.info("Shutdown hook");
            consumer.wakeup();
        }
    }
}
