package org.kafkabinaries.streamer;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class TweetFilterStream {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "try-stream-filter");


        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<Object, Object> inputStream = streamsBuilder.stream("Twitter_tweets_topic");
        KStream<Object, Object> filteredStream = inputStream.filter(
                (key, jsonTweet) -> extractFollowersCountFromTweet(jsonTweet.toString()) > 1000);

        filteredStream.to("Twitter_filtered_topic");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        kafkaStreams.start();
    }



    private static Integer extractFollowersCountFromTweet(String tweet) {

        try {
            return JsonParser.parseString(tweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (Exception e) {
            return 0;
        }
    }
}
