package com.yumtao.consume;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * 
 * 消费端具体业务消费
 * 
 * @author yumTao
 *
 */
public class Consumer implements Runnable {
	private KafkaStream kafka_steam;
	private int threadNum;

	public Consumer(KafkaStream a_stream, int a_threadNumber) {
		threadNum = a_threadNumber;
		kafka_steam = a_stream;
	}

	public void run() {
		ConsumerIterator<byte[], byte[]> it = kafka_steam.iterator();
		while (it.hasNext()) {
			String message = new String(it.next().message());
			// TODO 在此执行具体业务，消费消息
			System.out.println("Thread " + threadNum + ": " + message);
		}
		System.out.println("Shutting down Thread: " + threadNum);
	}
}