package io.uve.yypush.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 
 * @author yangyang21@staff.weibo.com(yangyang)
 * 
 */
public class KafkaClient {
	private final KafkaProducer<byte[], byte[]> producer;
	// private final AtomicInteger refs = new AtomicInteger();

	public KafkaClient(Properties kafkaProducerProps) {
		this.producer = new KafkaProducer<byte[], byte[]>(kafkaProducerProps);
	}

	public void close() {
		producer.close();
	}

	public void send(String topic, byte[] msg) {
		producer.send(new ProducerRecord<byte[], byte[]>(topic, msg));
	}
}
