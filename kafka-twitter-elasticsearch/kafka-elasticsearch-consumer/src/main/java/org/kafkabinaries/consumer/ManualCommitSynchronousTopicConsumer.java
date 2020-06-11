package org.kafkabinaries.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ManualCommitSynchronousTopicConsumer {
    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer();
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));



            for (ConsumerRecord<String, String> record : records) {
                log.info("Key : {}", record.key());
                log.info("Partition : {}, Offset : {}", record.partition(), record.offset());

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    log.error("Thread sleep disrupted - {}", e);
                }
            }
            log.info("Committing Offsets ");
            consumer.commitSync();
            log.info("Offsets Committed ");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Thread sleep disrupted - {}", e);
            }
        }
    }

    public static KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-elasticsearch");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //Disable auto commit
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("Twitter_tweets_topic"));
        return consumer;
    }
}
