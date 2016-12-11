package com.sreedhar.kafka101.producer;

public class KafkaProducerDemo {
    public static void main(String[] args) {
        String topicName = "test";
        final boolean isAsync = args.length <= 0 || !args[0].trim().toLowerCase().equals("sync");

        Producer producerThread = new Producer(topicName, isAsync);
        producerThread.start();

    }

}
