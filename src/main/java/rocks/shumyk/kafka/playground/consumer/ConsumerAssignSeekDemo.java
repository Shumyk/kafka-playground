package rocks.shumyk.kafka.playground.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static rocks.shumyk.kafka.playground.util.CommonUtil.createConsumerProperties;

public class ConsumerAssignSeekDemo {

	private static final Logger log = LoggerFactory.getLogger(ConsumerAssignSeekDemo.class);

	public static void main(String[] args) {
		// create consumer properties
		final String topic = "java_topic";
		final Properties properties = createConsumerProperties();

		// create consumer
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

			// assign and seek are mostly used to replay data or fetch a specific message
			// assign
			final TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
			final long offsetToReadFrom = 15L;
			consumer.assign(Collections.singletonList(partitionToReadFrom));
			// seek
			consumer.seek(partitionToReadFrom, offsetToReadFrom);

			int numberOfMessagesToRead = 5;
			boolean keepOnReading = true;
			int numberOfMessagesReadSoFar = 0;

			// poll for new data
			while (keepOnReading) {
				final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					numberOfMessagesReadSoFar++;
					log.info("Received record, key: [{}], value: [{}], partition: [{}], offset: [{}]", record.key(), record.value(), record.partition(), record.offset());

					if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
						keepOnReading = false; // to exit the while loop
						break; // to exit the for loop
					}
				}
			}
		}
		log.info("Exiting the consumer");
	}
}
