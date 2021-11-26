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
    private final static String TOPIC_NAME = "test1";
    private final static String BOOTSTRAP_SERVERS = "hLinux:9092";
    private final static String GROUP_ID = "test-group1";
    private final static int PARTITION_NUMBER = 0;

    private static KafkaConsumer<String, String> consumer = null;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutDownThread()); // 안전한 컨슈머 종료를 위해 셧다운 훅을 사용하면 Shutdown 스레드 메서드를 호출한다.
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 명시적 오프셋 커밋을 하겠다는 설정

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalancedListener()); // 랜덤 파티션 할당
        Set<TopicPartition> assignmentPartition = consumer.assignment(); // 컨슈머에 할당된 파티션 확인 방법
        // consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER))); // 토픽과 파티션넘버를 명시적으로 지정

        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("==> {}", record);

                    // 동기식 오프셋 커밋에 필요 START
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1 , null)); // 개별 오프셋 커밋
                     consumer.commitSync(currentOffsets); // 동기 오프셋 커밋
                    // 동기식 오프셋 커밋에 필요 END

                    // 비동기식 오프셋 커밋 START
//                    consumer.commitAsync(new OffsetCommitCallback() {
//                        @Override
//                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//                            if (exception != null) {
//                                System.err.println("Commit failed");
//                            } else {
//                                System.out.println("Commit succeeded");
//                            }
//                            if (exception != null)
//                                logger.error("Commit failed for offsets {}", offsets, exception);
//                        }
//                    });
                    // 비동기식 오프셋 커밋 END
                }
            }
        } catch (WakeupException e) {
            // wakeup() 메서드가 실행된 후 poll() 메서드를 실행하면 여길로 오게 된다.
            logger.warn("Wakeup consumer");
            // 리소스 종료 처리
        } finally {
            consumer.close();
        }
    }

    public static class RebalancedListener implements ConsumerRebalanceListener {
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

    // 셧다운 훅을 받아 처리하는 부분
    private static class ShutDownThread extends Thread {
        public void run() {
            logger.info("Shutdown hook");
            consumer.wakeup();
        }
    }
}
