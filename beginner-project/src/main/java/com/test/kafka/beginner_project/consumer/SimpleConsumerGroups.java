package com.test.kafka.beginner_project.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumerGroups {

	final static Logger logger = LoggerFactory.getLogger(SimpleConsumerGroups.class);

	public static void main(String[] args) {
		final String groupId = "Demo-Application";
		final Properties properties = new Properties();

		// create consumer config
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// create consumer
		final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

		// subscribe to topic(s)
		kafkaConsumer.subscribe(Collections.singleton("topic1"));

		// Poll for new data
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				logger.info("Message Consumed. Key={}, Value={},partitions={}, offset={}", record.key(), record.value(),
						record.partition(), record.offset());
			}
		}

	}

}
