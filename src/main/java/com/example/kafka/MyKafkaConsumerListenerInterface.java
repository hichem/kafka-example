package com.example.kafka;

public interface MyKafkaConsumerListenerInterface {

	void onNewMessageReceived(String topic, String message);
	
}
