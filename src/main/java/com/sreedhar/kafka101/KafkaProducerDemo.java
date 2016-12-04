package com.sreedhar.kafka101;

public class KafkaProducerDemo implements KafkaProperties {
	public static void main(String [] args){
		final boolean isAsync = args.length <= 0 || !args[0].trim().toLowerCase().equals("sync");

		Producer producerThread = new Producer(KafkaProperties.topic, isAsync);
		producerThread.start();

	}

}
