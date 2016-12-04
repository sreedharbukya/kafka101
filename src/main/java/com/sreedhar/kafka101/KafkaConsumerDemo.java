package com.sreedhar.kafka101;

public class KafkaConsumerDemo implements KafkaProperties {
	public static void main(String[] args) {
		Consumer consumerThread = new Consumer(KafkaProperties.topic);
		consumerThread.start();

	}
}