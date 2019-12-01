package com.github.yang.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {
	public static RestHighLevelClient createClient() {
		String hostname = "";
		String username = "";
		String password = "";
		
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	
	public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
		String bootstrapServers = "192.168.1.102:9092";
		String groupId = "twitter-group";
		
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
		
	}
	
	private static JsonParser jsonParser = new JsonParser();
	private static String extractIdFromTweet(String tweetJson) {
		return jsonParser.parse(tweetJson)
				.getAsJsonObject()
				.get("id_str")
				.getAsString();
	}
	
	public static void main(String[] args) throws IOException {
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		RestHighLevelClient client = createClient();
		
		KafkaConsumer<String, String> consumer = createKafkaConsumer("important-tweets");
		while ( true ) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			Integer recordCount = records.count();
			logger.info("Received "+ recordCount +" records");
			
			BulkRequest bulkRequest = new BulkRequest();
			
			for ( ConsumerRecord<String, String> record : records ) {
				try {
					String id = extractIdFromTweet(record.value());
					IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(record.value(), XContentType.JSON);
					
					// Write individually to ElasticSearch
					//IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
					bulkRequest.add(indexRequest);
				} catch (NullPointerException e) {
					logger.warn("Skipping bad data: "+record.value());
				}
			}
			
			if ( recordCount > 0 ) {
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				
				logger.info("Committing offsets...");
				consumer.commitSync();
				logger.info("Offsets have been committed");
			}
			
			try {
				Thread.sleep(1000);
			} catch ( InterruptedException e ) {
				e.printStackTrace();
			}
		}
		
		//client.close();
	}
}
