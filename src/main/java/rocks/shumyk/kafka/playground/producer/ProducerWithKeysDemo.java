package rocks.shumyk.kafka.playground.producer;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static rocks.shumyk.kafka.playground.util.CommonUtil.createProperties;

public class ProducerWithKeysDemo {

	private static final Logger logger = LoggerFactory.getLogger(ProducerWithKeysDemo.class);

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		// create producer props
		final Properties properties = createProperties();

		// create the producer
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			for (int i = 0; i < 10; i++) {
				// send messages
				final ProducerRecord<String, String> record = createMessage(i);
				logger.info("Key: {}", record.key());
				// id-0 -> partition 0
				// id-1 -> partition 0
				// id-2 -> partition 2
				// id-3 -> partition 2
				// id-4 -> partition 0
				// id-5 -> partition 0
				// id-6 -> partition 0
				// id-7 -> partition 2
				// id-8 -> partition 0
				// id-9 -> partition 2

				producer.send(record, messageCallback())
					.get(); // block the .send() to make it synchronous - don't do this in production!
				producer.flush();
			}
		}
	}

	private static ProducerRecord<String, String> createMessage(final int i) {
		final String topic = "java_topic";
		final String key = "id-" + i;
		final String value = "this message is actually from java mate, " + i;
		return new ProducerRecord<>(topic, key, value);
	}

	private static Callback messageCallback() {
		return (recordMetadata, e) -> {
			// executes every time a record is successfully sent or an exception is thrown
			if (Objects.isNull(e))
				// the record was successfully sent
				logger.info("Received new metadata.\n Topic: {}.\n Partitions: {}.\n Offset: {}.\n Timestamp: {}.", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
			else {
				logger.error("Error while producing", e);
			}
		};
	}
}
