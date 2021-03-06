package com.test.kafka.beginner_project.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumerThread {

	final static Logger logger = LoggerFactory.getLogger(SimpleConsumerThread.class);

	public static void main(String[] args) throws InterruptedException {

		final CountDownLatch latch = new CountDownLatch(1);

		Thread thread = new Thread(new ConsumerThread(latch));
		thread.start();

		latch.await();

	}

	public static class ConsumerThread implements Runnable {
		final static Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
		private CountDownLatch latch;
		private KafkaConsumer<String, String> kafkaConsumer;

		public ConsumerThread(CountDownLatch latch) {
			this.latch = latch;

			final String groupId = "Demo-Application-thread";
			final Properties properties = new Properties();

			// create consumer config
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			kafkaConsumer = new KafkaConsumer<String, String>(properties);
			kafkaConsumer.subscribe(Collections.singleton("topic1"));
		}

		public void run() {

			try {
				while (true) {
					ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord<String, String> record : records) {
						logger.info("Message Consumed. Key={}, Value={},partitions={}, offset={}", record.key(),
								record.value(), record.partition(), record.offset());
					}
				}
			} catch (WakeupException wakeupException) {
				logger.error("Info received shutdown signal");
			} finally {
				kafkaConsumer.close();
				// tell our main code we are done with consumer
				latch.countDown();
			}

		}

		public void shutdown() {
			// Special method to interrub consumer poll
			kafkaConsumer.wakeup();
		}

	}

}
