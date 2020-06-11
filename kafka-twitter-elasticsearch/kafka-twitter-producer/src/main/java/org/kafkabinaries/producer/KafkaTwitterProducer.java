package org.kafkabinaries.producer;

import com.twitter.hbc.core.Client;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
@NoArgsConstructor
public class KafkaTwitterProducer {

    public static void main(String[] args) {
        new KafkaTwitterProducer().run();
    }

    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // create twitter client
        TwitterClient twitterClient = new TwitterClient();
        Client client = twitterClient.createTwitterClient(msgQueue);
        client.connect();



        // create kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        // message count tracker
        int count = 0;


        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                client.stop();
                log.error("InterruptedException", e);
            }

            if (null != msg) {
                log.info("Message : {}", msg);
                kafkaProducer.send(new ProducerRecord<>("Twitter_tweets_topic", "_id_"+count, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        log.error(" Error sending Kafka message - {}", e);
                    }
                } );
            }
            count++;
        }

        // Add shutdown hook
        int finalCount = count;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down twitter producer");
            client.stop();
            kafkaProducer.close();
            log.info("twitter producer successfully closed");
            log.info("Total tweets processed - {}", finalCount);
        }));
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Properties for a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // The above property is equivalent to setting the below 3 propertiess
        //properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        //properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        //properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High throughput properties
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "25");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        return kafkaProducer;
    }


}
