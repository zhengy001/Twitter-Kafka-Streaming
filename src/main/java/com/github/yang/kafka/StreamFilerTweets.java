package com.github.yang.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class StreamFilerTweets {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.102:9092");
		props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "twitter-stream");
		props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());

		StreamsBuilder streamBuilder = new StreamsBuilder();

		KStream<String, String> inputTopic = streamBuilder.stream("twitter-tweets");

		KStream<String, String> filteredStream = inputTopic.filter(
				// filter for tweets which has a suer of over 1000 follower
				(k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) >= 1000
		);
		
		filteredStream.to("important-tweets");
		
		KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder.build(), props);
		kafkaStreams.start();

	}

	private static JsonParser jsonParser = new JsonParser();

	private static Integer extractUserFollowersInTweet(String tweetJson) {
		try {
			return jsonParser.parse(tweetJson)
				   .getAsJsonObject()
				   .get("user")
				   .getAsJsonObject()
				   .get("followers_count")
				   .getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}

	}

}
