package rocks.shumyk.kafka.playground;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ProducerWiithCallbackDemo {


	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(ProducerWiithCallbackDemo.class);

		// create producer props
		final Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			for (int i = 0; i < 10; i++) {
				// send messages
				ProducerRecord<String, String> record = new ProducerRecord<>("java_topic", "this message is actually from java mate, " + i);
				producer.send(record, (recordMetadata, e) -> {
					// executes every time a record is successfully sent or an exception is thrown
					if (Objects.isNull(e))
						// the record was successfully sent
						logger.info("Received new metadata.\n Topic: {}.\n Partitions: {}.\n Offset: {}.\n Timestamp: {}.", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
					else {
						logger.error("Error while producing", e);
					}

				});
				producer.flush();
			}
		}
	}
}
