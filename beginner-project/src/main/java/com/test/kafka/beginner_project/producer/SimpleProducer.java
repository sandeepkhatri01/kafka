package com.test.kafka.beginner_project.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Hello world!
 *
 */
public class SimpleProducer {
	public static void main(String[] args) {

		// Create producer properties
		final Properties properties = new Properties();
		String bootStrapServers = "127.0.0.1:9092";

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create producer
		final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

		// Create a ProducerRecord
		final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("topic1",
				"Hello World1");
		// send message -- asynchronous
		kafkaProducer.send(producerRecord);
		
		//flush Data
		kafkaProducer.flush();
		
		//Flush and close producer
		kafkaProducer.close();
	}
}
