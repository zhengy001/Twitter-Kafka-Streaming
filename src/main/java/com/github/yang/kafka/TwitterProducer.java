package com.github.yang.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	private String consumerKey = "";
	private String consumerSecret = "";
	private String token = "";
	private String secret = "";
	
	private String bootstrapServers = "192.168.1.102:9092";
	
	private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	public TwitterProducer() {
	}

	public void run() {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		logger.info("Start");
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		// Create twitter client
		Client hosebirdClient = createTwitterClient(msgQueue);
		// Attempts to establish a connection.
		hosebirdClient.connect();
		
		// Create Kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		
		Runtime.getRuntime().addShutdownHook(new Thread(() ->  {
			logger.info("stopping application...");
			
			hosebirdClient.stop();
			producer.close();
		}));
		
		while (!hosebirdClient.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				hosebirdClient.stop();
			}

			if (msg != null) {
				logger.info(msg);
				
				producer.send(new ProducerRecord<String, String>("twitter-tweets", null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if ( exception != null ) {
							logger.error("Error", exception);
						}
					}
				});
			}
		}
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		// List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("bitcoin");
		// hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		
		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
				
		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
	
	public KafkaProducer<String, String> createKafkaProducer() {
		Properties props = new Properties();
		
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		//props.setProperty(ProducerConfig.BO, value)
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Safe producer
		props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		// Blows have implicit setting by using idempotence 
		props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		
		// High throughput producer
		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		
		return producer;
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}
}
