package com.example.kafka;




public class Main {
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		MyLogger.getSharedLogger().logInfo("[Main::main] Kafka Example Started ");
		
		//Initialize Global Config
		GlobalConfig config = GlobalConfig.getInstance();
		
		//Create a kafka admin
		MyKafkaAdmin kafkaAdmin = MyKafkaAdmin.getInstance(config.getZookeeperConnect());
		
		//Create a topic
		final String TEST_TOPIC = "TEST_TOPIC";
		kafkaAdmin.createTopic(TEST_TOPIC);
		
		//Create Kafka Consumer
		MyKafkaConsumerManager kafkaConsumerManager = MyKafkaConsumerManager.getInstance(config.getKafkaConnect(), config.getKafkaConsumerGroup());
		
		//Add consumer for test topic
		kafkaConsumerManager.addConsumer(TEST_TOPIC, new MyKafkaConsumerListenerInterface() {
			
			public void onNewMessageReceived(String topic, String message) {
				
				MyLogger.getSharedLogger().logInfo(String.format("Consumer Rcv <-- Topic: %s, Message: %s", topic, message));
				
			}
		});
		
		//Create kafka producer
		MyKafkaProducer kafkaProducer = MyKafkaProducer.getInstance(config.getKafkaConnect());
		
		//Write message to kafka test topic
		for(int i = 0; i < 10; i++) {
			kafkaProducer.writeMessageToTopic(TEST_TOPIC, String.format("Message # %d", i));
		}
		
		//Let the program run for 10 seconds
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
