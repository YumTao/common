package com.yumtao.product;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.yumtao.Config;

public class ProducerAsync {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.servers", Config.BROKER_URL);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("producer.type", "async");
		props.put("batch.size", "16384");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		int msgIndex = 0;
		while (true) {
			String message = "Async : this is the " + msgIndex + "th message for test!";

			ProducerRecord producerRecord = new ProducerRecord(Config.TOPIC, message);
			producer.send(producerRecord);

			System.out.println("send msg :" + message);
			try {
				Thread.sleep(Config.PRODUCT_GRAP);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

//		producer.close();
	}
}