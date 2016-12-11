package com.sreedhar.kafka101.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

class Producer extends Thread implements Callback {
    private final KafkaProducer<Integer, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }


    public void run() {
        int messageNo = 1;
        while (true) {
            String messageStr = "Hello_" + messageNo;
            if (isAsync) {
                producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr), this);
            } else {
                try {
                    producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr)).get();
                    System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            System.out.println("Sent data via aSync  Topic " + metadata.topic() + " Parititon  " + metadata.partition() + " Offset " + metadata.offset());
        } else {
            exception.printStackTrace();
        }
    }
}

