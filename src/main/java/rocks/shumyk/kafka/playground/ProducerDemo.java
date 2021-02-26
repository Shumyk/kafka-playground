package rocks.shumyk.kafka.playground;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

	public static void main(String[] args) {
		// create producer props
		final Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			// send messages
			ProducerRecord<String, String> record = new ProducerRecord<>("java_topic", "this message is actually from java  mate");
			producer.send(record);
			producer.flush();
		}
	}
}
