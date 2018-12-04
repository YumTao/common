package com.yumtao.product;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.yumtao.Config;

public class ProducerMyPartition {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.servers", Config.BROKER_URL);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("producer.type", "async");
		props.put("batch.size", "16384");
		props.put("partitioner.class", "com.yumtao.product.MyPartition");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		int msgIndex = 0;
		while (true) {
			String message = "";
			for (int i = 0; i < 10; i++) {
				message += msgIndex;
			}

			ProducerRecord producerRecord = new ProducerRecord(Config.TOPIC, String.valueOf(msgIndex), message);
			producer.send(producerRecord);

			System.out.println(String.format("send partition: %s, msg : %s", msgIndex, message));
			try {
				Thread.sleep(Config.PRODUCT_GRAP);
				msgIndex = (msgIndex + 1) % 2;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

//		producer.close();
	}
}