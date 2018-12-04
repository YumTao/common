package com.yumtao.product.mypartition;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.yumtao.Config;

/**
 * 生产端：自定义partition策略
 * @author yumTao
 *
 */
public class ProducerMyPartition {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("bootstrap.servers", Config.BROKER_URL);
		// key的序列化类
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// value的序列化类
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("producer.type", "async");
		props.put("batch.size", "16384");
		// 自定义partition策略
		props.put("partitioner.class", "com.yumtao.product.MyPartition");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		int msgIndex = 0;
		while (true) {
			String message = "";
			for (int i = 0; i < 10; i++) {
				message += msgIndex;
			}

			// ProducerRecord(String topic, V value): 随机发送消息
			// ProducerRecord(String topic, K key, V value) ： 指定key，根据partition策略发送消息
			// ProducerRecord(String topic, Integer partition, K key, V value)： 指定发送到哪个partition中
			ProducerRecord producerRecord = new ProducerRecord(Config.TOPIC, String.valueOf(msgIndex), message);
			producer.send(producerRecord);

			System.out.println(String.format("send partition: %s, msg : %s", msgIndex, message));
			try {
				Thread.sleep(Config.PRODUCT_GRAP);
				msgIndex = (msgIndex + 1) % 2;
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		// 关流
//		producer.close();
	}
}