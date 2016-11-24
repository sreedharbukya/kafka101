package com.alacriti.kafka102;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;



public class TestProducer {
	public static void main(String args[]) throws InterruptedException, ExecutionException {
		Properties props = new Properties();
//		SimpleConsumerShell

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		try {

			String topic = "topic";
			while (true) {
				ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, null, UUID.randomUUID().toString());
				Future<RecordMetadata> recordMetaData = producer.send(producerRecord);
				System.out.println("Success");
			}

		} catch (Exception e) {
			e.printStackTrace();
			producer.close();
		}


	}
}