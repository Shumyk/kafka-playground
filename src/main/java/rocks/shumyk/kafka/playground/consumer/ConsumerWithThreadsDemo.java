package rocks.shumyk.kafka.playground.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.singleton;
import static rocks.shumyk.kafka.playground.util.CommonUtil.createConsumerProperties;

public class ConsumerWithThreadsDemo {

	private static final Logger log = LoggerFactory.getLogger(ConsumerWithThreadsDemo.class);

	public static void main(String[] args) {
		// latch for dealing with multiple threads
		final CountDownLatch latch = new CountDownLatch(1);

		// create consumer runnable
		log.info("Creating the consumer thread");
		final String groupId = "NOT_last_consumer_group";
		final String topic = "java_topic";
		final ConsumerThread consumerThread = new ConsumerThread(latch, groupId, topic);

		// start the thread
		new Thread(consumerThread).start();

		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(runtimeShutdownHook(latch, consumerThread));
		latchAwait(latch);
	}

	private static Thread runtimeShutdownHook(final CountDownLatch latch, final ConsumerThread consumerThread) {
		return new Thread(() -> {
			log.info("Caught shutdown hook");
			consumerThread.shutdown();
			latchAwait(latch);
			log.info("Consuming has exited");
		});
	}

	private static void latchAwait(final CountDownLatch latch) {
		try {
			latch.await();
		} catch (Exception e) {
			log.error("Unexpected error occurs during consuming: {}", e.getMessage(), e);
		} finally {
			log.info("Consuming is closing");
		}
	}


	public static class ConsumerThread implements Runnable {

		private final CountDownLatch latch;
		private final KafkaConsumer<String, String> consumer;

		public ConsumerThread(final CountDownLatch latch, final String groupId, final String... topics) {
			this.latch = latch;
			// create consumer properties
			final Properties properties = createConsumerProperties(groupId);
			// create consumer
			this.consumer = new KafkaConsumer<>(properties);
			// subscribe consumer to out topic(s)
			consumer.subscribe(Arrays.asList(topics));
		}

		@Override
		public void run() {
			try {
				while (true) processConsuming();
			} catch (WakeupException ex) {
				log.info("Received shutdown signal!");
			} finally {
				consumer.close();
				// tell our main code we're done with the consumer
				latch.countDown();
			}
		}

		private void processConsuming() {
			final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				log.info("Received record, key: [{}], value: [{}], partition: [{}], offset: [{}]", record.key(), record.value(), record.partition(), record.offset());
			}
		}

		public void shutdown() {
			// the wakeup() method is special method to interrupt consumer.poll()
			// it will throw the exception WakeUpException
			consumer.wakeup();
		}
	}
}
