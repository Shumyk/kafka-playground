package rocks.shumyk.kafka.playground;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static rocks.shumyk.kafka.playground.util.CommonUtil.createProperties;

public class ProducerDemo {

	public static void main(String[] args) {
		// create producer props
		final Properties properties = createProperties();

		// create the producer
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			// send messages
			ProducerRecord<String, String> record = new ProducerRecord<>("java_topic", "this message is actually from java, mate");
			producer.send(record);
			producer.flush();
		}
	}
}
