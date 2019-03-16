package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;



public class MyKafkaProducer {



	//Singleton instance
	private static MyKafkaProducer				g_sharedKafkaProducer		= null;


	private KafkaProducer<String, String> 		m_producer					= null;


	//Public Shared Constructor
	public static MyKafkaProducer getInstance(String kafkaConnect) {
		if(g_sharedKafkaProducer != null) {
			synchronized(MyKafkaProducer.class) {
				if(g_sharedKafkaProducer == null) {
					g_sharedKafkaProducer = new MyKafkaProducer(kafkaConnect);
				}
			}
		}

		return g_sharedKafkaProducer;
	}


	//Private constructor
	private MyKafkaProducer(String kafkaConnect) {
		initialize(kafkaConnect);
	}



	private boolean initialize(String kafkaConnect) {
		MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaProducer::initialize]"));

		boolean result = true;

		if(m_producer == null) {
			if((kafkaConnect == null) || (kafkaConnect.isEmpty())) {
				MyLogger.getSharedLogger().logError(String.format("[MyKafkaProducer::initailize] Kafka Connect is NULL"));
				result = false;
			} else {
				// create instance for properties to access producer configs   
				Properties props = new Properties();

				//Kafka connection parameters
				props.put("bootstrap.servers", kafkaConnect);

				//Set acknowledgements for producer requests.      
				props.put("acks", "all");

				//If the request fails, the producer can automatically retry,
				props.put("retries", 0);

				//Specify buffer size in config
				//props.put("request.timeout.ms", "5000");

				props.put("batch.size", 1024);

				//Reduce the no of requests less than 0   
				props.put("linger.ms", 100);

				//The buffer.memory controls the total amount of memory available to the producer for buffering.   
				props.put("buffer.memory", 33554432);

				props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

				props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

				//Create the kafka producer
				try {
					m_producer = new KafkaProducer<String, String>(props);

				} catch (Exception ex) {
					MyLogger.getSharedLogger().logError(String.format("[MyKafkaProducer::initailize] Error: %s", ex.getMessage()));
					result = false;
				}
			}
		}

		return result;
	}


	//Close Kafka Producer
	public void close() {
		MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaProducer::close]"));

		//Close producer
		m_producer.close();

	}


	//Write message to topic
	public boolean writeMessageToTopic(String topic, String message) {
		MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaProducer::writeMessageToTopic] Topic: %s, Message: %s", topic, message));

		boolean result = true;

		//Send message
		try {
			m_producer.send(new ProducerRecord<String, String>(topic, message));
		} catch (Exception ex) {
			MyLogger.getSharedLogger().logError(String.format("[MyKafkaProducer::writeMessageToTopic] Error: %s", ex.getMessage()));
			result = false;
		}

		return result;
	}

}


