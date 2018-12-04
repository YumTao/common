package com.yumtao.product;

import kafka.producer.Partitioner;

/**
 * @goal 自定义partition
 * @author yumTao
 *
 */
public class MyPartition implements Partitioner{

	/**
	 * 返回的值为对应的partition位置如 partition-0
	 */
	@Override
	public int partition(Object key, int numPartitions) {
		int keyValue = Integer.valueOf(key.toString());
		return keyValue % 2;
	}

}
