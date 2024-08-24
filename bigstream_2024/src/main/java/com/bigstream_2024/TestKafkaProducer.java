package com.bigstream_2024;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestKafkaProducer {

	public static void main(String[] args) throws Exception {

		
		GenerateStreamData.generateRandomStreamData(1000); 
	

	}

	public static Producer<String, String> sendDataToKafka(String topicName, String event) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		producer.send(new ProducerRecord<String, String>(topicName, event));
		return producer;
	}
	public static Producer<String, String> sendDataToKafka(String topicName, String event, String key) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
			
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, event);
		producer.send(record);
		return producer;
	}

	
}