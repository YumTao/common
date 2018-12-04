package com.yumtao;

public class Config {

	// common config
	public static final String TOPIC = "test_partiton";

	// product config
	public static final String BROKER_URL = "singlenode:9092";
	public static final long PRODUCT_GRAP = 1000l;

	// consumer config
	public static final String ZK_URL = "singlenode:2181";
	public static final String CONSUMER_GROUP = "group_partition";
	public static final String THREAD_NUMS = "1";

}
