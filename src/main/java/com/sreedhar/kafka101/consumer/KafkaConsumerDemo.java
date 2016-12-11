package com.sreedhar.kafka101.consumer;

public class KafkaConsumerDemo{
	public static void main(String[] args) {
		String topicName = "test";
		Consumer consumerThread = new Consumer(topicName);
		consumerThread.start();

	}
}