package com.yumtao.consume;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.yumtao.Config;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class ConsumerGroup {
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;

	public ConsumerGroup(String zk_url, String groupId, String topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zk_url, groupId));
		this.topic = topic;
	}

	/**
	 * 创建客户端配置
	 * 
	 * @param zk_url
	 * @param groupId
	 * @return
	 */
	private static ConsumerConfig createConsumerConfig(String zk_url, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zk_url);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}

	/**
	 * 消费端拉取消息
	 * 
	 * @param numThreads group的consumer数
	 */
	public void run(int numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		executor = Executors.newFixedThreadPool(numThreads);

		int threadNumber = 0;
		for (final KafkaStream stream : streams) {
			executor.submit(new Consumer(stream, threadNumber));
			threadNumber++;
		}
	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
		try {
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public static void main(String[] args) {
		String zooKeeper = Config.ZK_URL;
		String groupId = Config.CONSUMER_GROUP;
		String topic = Config.TOPIC;
		int threads = Integer.parseInt(Config.THREAD_NUMS);

		ConsumerGroup example = new ConsumerGroup(zooKeeper, groupId, topic);
		example.run(threads);

		try {
			Thread.sleep(10000);
		} catch (InterruptedException ie) {

		}
		example.shutdown();
	}
}