package rocks.shumyk.kafka.playground.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static java.util.Collections.singleton;
import static rocks.shumyk.kafka.playground.util.CommonUtil.createConsumerProperties;

public class ConsumerWithGroupsDemo {

	private static final Logger log = LoggerFactory.getLogger(ConsumerWithGroupsDemo.class);

	public static void main(String[] args) {
		// create consumer properties
		final String groupId = "NOT_last_consumer_group";
		final Properties properties = createConsumerProperties(groupId);

		// create consumer
		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		// subscribe consumer to out topic(s)
		consumer.subscribe(singleton("java_topic"));

		// poll for new data
		while (true) {
			final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				log.info("Received record, key: [{}], value: [{}], partition: [{}], offset: [{}]", record.key(), record.value(), record.partition(), record.offset());
			}
		}
	}
}
