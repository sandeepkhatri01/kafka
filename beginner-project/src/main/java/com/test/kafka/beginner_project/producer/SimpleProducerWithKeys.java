package com.test.kafka.beginner_project.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducerWithKeys {
	final static Logger logger = LoggerFactory.getLogger(SimpleProducerWithKeys.class);

	public static void main(String[] args) {

		// Create producer properties
		final Properties properties = new Properties();
		String bootStrapServers = "127.0.0.1:9092";

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create producer
		final KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

		final String key = "key1";
		// Create a ProducerRecord
		final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("topic1",key,
				"Hello World1");
		// send message -- asynchronous
		kafkaProducer.send(producerRecord, new Callback() {

			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// executes every time record is successfully send or an exception is thrown
				if (exception == null) {
					// record was successfully sent
					logger.info("Received Meta Data. Topic={}, Partition={}, offsets={}, and TimeStamp={}",
							metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
				} else {

				}
			}
		});

		// flush Data
		kafkaProducer.flush();

		// Flush and close producer
		kafkaProducer.close();
	}
}
