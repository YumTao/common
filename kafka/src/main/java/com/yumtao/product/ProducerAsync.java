package com.yumtao.product;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerAsync {

	private static final String TOPIC = "first2";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Properties props = new Properties();
		props.put("bootstrap.servers", "singlenode:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("producer.type", "async");
		props.put("batch.size", "16384");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 100; i++) {

			String message = "Async : this is the " + i + "th message for test!";

			ProducerRecord producerRecord = new ProducerRecord(TOPIC, message);
			producer.send(producerRecord);
			System.out.println("send msg :" + message);

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		producer.close();
	}
}