package rocks.shumyk.kafka.playground.producer;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

import static rocks.shumyk.kafka.playground.util.CommonUtil.createProperties;

public class ProducerWithCallbackDemo {

	private static final Logger logger = LoggerFactory.getLogger(ProducerWithCallbackDemo.class);

	public static void main(String[] args) {
		// create producer props
		final Properties properties = createProperties();
		// create the producer
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			for (int i = 0; i < 10; i++) {
				// send messages
				ProducerRecord<String, String> record = new ProducerRecord<>("java_topic", "this message is actually from java, mate, " + i);
				producer.send(record, messageCallback());
				producer.flush();
			}
		}
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
