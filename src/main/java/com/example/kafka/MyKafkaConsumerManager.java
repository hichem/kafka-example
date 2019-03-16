package com.example.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.exception.ZkTimeoutException;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class MyKafkaConsumerManager {

	private String												m_zookeeperIP				= null;
	private String												m_groupID					= null;
	private Map<String, KafkaConsumerThread> 					m_topicListeners			= new HashMap<String, KafkaConsumerThread> ();
	private Map<String, ConsumerConnector>						m_kafkaConsumers			= new HashMap<String, ConsumerConnector>();
	private static Properties									m_consumerProps				= null;
	private static ConsumerConfig								m_consumerConfig			= null;


	private static MyKafkaConsumerManager		g_sharedKafkaConsumer		= null;


	//Shared Constructor
	public static MyKafkaConsumerManager getInstance(String zookeeperConnect, String consumerGroupId) {
		if(g_sharedKafkaConsumer == null) {
			synchronized(MyKafkaConsumerManager.class) {
				g_sharedKafkaConsumer = new MyKafkaConsumerManager(zookeeperConnect, consumerGroupId);
			}
		}

		return g_sharedKafkaConsumer;
	}

	public MyKafkaConsumerManager(String zookeeperConnect, String consumerGroup) {
		
		m_groupID = consumerGroup;
		m_zookeeperIP = zookeeperConnect;
		
		initializeKafkaConsumer(zookeeperConnect, consumerGroup);
		
	}

	

	//Initialize kafka consumer
	private void initializeKafkaConsumer(String zookeeperIP, String groupID) {
		MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaConsumerManager::initializeKafkaConsumer] ZookeeperConnect: %s, Group ID: %s", zookeeperIP, groupID));


		m_zookeeperIP		= zookeeperIP;
		m_groupID			= groupID;

		createConsumerConfig(m_zookeeperIP, m_groupID);
	}


	//Close consumer
	void closeKafkaConsumer() {
		MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaConsumerManager::closeKafkaConsumer]"));

		for(ConsumerConnector consumer : m_kafkaConsumers.values()) {
			consumer.shutdown();
		}
	}

	//Add topic to topic list
	public boolean addConsumer(String topic, MyKafkaConsumerListenerInterface listener) {
		MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaConsumerManager::addConsumer] Topic: %s", topic));

		boolean result = true;

		if((topic == null) || (topic.isEmpty())) {
			MyLogger.getSharedLogger().logError(String.format("[MyKafkaConsumerManager::addConsumer] Error: Trying to add an empty topic to kafka"));
			result = false;
		} else if (listener == null) {
			MyLogger.getSharedLogger().logError(String.format("[MyKafkaConsumerManager::addConsumer] Error: Provided listener is empty"));
			result = false;
		} else {

			//Create a new topic hash map
			Map<String, Integer> _newTopic = new HashMap<String, Integer>();
			_newTopic.put(topic, 1);	//1 stream per topic

			Map<String, List<KafkaStream<byte[], byte[]>>> _newStreamMap = null;
			try {

				//Check if the topic already exist
				if(m_topicListeners.containsKey(topic) == true) {

					//The topic is already present, we don't need to recreate the streams >> Update the listener
					m_topicListeners.get(topic).setListener(listener);

				} else {
					//Add new consumer
					ConsumerConnector consumer = null;
					try {
						consumer = kafka.consumer.Consumer.createJavaConsumerConnector(m_consumerConfig);

						//Create stream for new topic
						_newStreamMap = consumer.createMessageStreams(_newTopic);

						//Create new thread for consumer
						KafkaConsumerThread consumerThread = new KafkaConsumerThread(topic, listener, _newStreamMap.get(topic));
						consumerThread.setName("Consumer_Thread_" + topic);
						consumerThread.start();

						//TODO: Wait for the thread to properly initialize the consumer

						//Update the topic / listener map
						m_topicListeners.put(topic, consumerThread);

						//consumer.setConsumerRebalanceListener(this);

						//Add consumer to list
						m_kafkaConsumers.put(topic, consumer);

					} catch (ZkTimeoutException ex) {
						MyLogger.getSharedLogger().logError(String.format("[MyKafkaConsumerManager::addConsumer] Error: %s", ex.getMessage()));
						result = false;
					} catch (Exception e) {
						MyLogger.getSharedLogger().logError(String.format("[MyKafkaConsumerManager::addConsumer] Error: %s", e.getMessage()));
						result = false;
					}
				}

			} catch (Exception ex) {
				MyLogger.getSharedLogger().logError(String.format("[MyKafkaConsumerManager::addConsumer] Failed to create streams to topic %s. Error: %s", topic, ex.getMessage()));
				result = false;
			}
		}

		return result;
	}



	//Add topic to topic list
	public ConsumerConnector getConsumer(String topic) {
		MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaConsumerManager::getConsumer] Topic: %s", topic));

		ConsumerConnector consumer = null;

		if((topic == null) || (topic.isEmpty())) {

			MyLogger.getSharedLogger().logError(String.format("[MyKafkaConsumerManager::getConsumer] Error: Trying to add an empty topic to kafka"));

		} else {

			//Create a new topic hash map
			Map<String, Integer> _newTopic = new HashMap<String, Integer>();
			_newTopic.put(topic, 1);	//1 stream per topic

			Map<String, List<KafkaStream<byte[], byte[]>>> _newStreamMap = null;
			try {

				try {
					consumer = kafka.consumer.Consumer.createJavaConsumerConnector(m_consumerConfig);

				} catch (ZkTimeoutException ex) {
					MyLogger.getSharedLogger().logError(String.format("[MyKafkaConsumerManager::getConsumer] Error: %s", ex.getMessage()));
				} catch (Exception e) {
					MyLogger.getSharedLogger().logError(String.format("[MyKafkaConsumerManager::getConsumer] Error: %s", e.getMessage()));
				}

			} catch (Exception ex) {
				MyLogger.getSharedLogger().logError(String.format("[MyKafkaConsumerManager::getConsumer] Failed to create streams to topic %s. Error: %s", topic, ex.getMessage()));
			}
		}

		return consumer;
	}


	//Create zookeeper config
	private static void createConsumerConfig(String a_zookeeper, String a_groupId) {
		MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaConsumerManager::createConsumerConfig]"));

		m_consumerProps = new Properties();
		m_consumerProps.put("zookeeper.connect", a_zookeeper);
		m_consumerProps.put("group.id", a_groupId);
		m_consumerProps.put("zookeeper.connection.timeout.ms", "6000");
		m_consumerProps.put("zookeeper.session.timeout.ms", "6000");
		m_consumerProps.put("zookeeper.sync.time.ms", "2000");
		m_consumerProps.put("rebalance.backoff.ms", "6000");
		m_consumerProps.put("auto.commit.interval.ms", "30000");
		m_consumerProps.put("auto.commit.enable", "false");
		m_consumerProps.put("consumer.timeout.ms", "100");
		m_consumerConfig = new ConsumerConfig(m_consumerProps);
	}

	public static ConsumerConfig getConsumerConfig() {
		return m_consumerConfig;
	}


	public boolean removeConsumer(String topic) {
		MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaConsumerManager::removeConsumer] Topic: %s", topic));

		boolean result = false;

		ConsumerConnector consumer = m_kafkaConsumers.get(topic);

		if(consumer != null) {

			//Close the consumer
			consumer.shutdown();

			//Remove the listener
			m_topicListeners.remove(topic);

			//Remove the consumer from the list of consumers
			m_kafkaConsumers.remove(topic);

		} else {
			MyLogger.getSharedLogger().logWarn(String.format("[MyKafkaConsumerManager::removeConsumer] No consumer for topic: %s", topic));
			result = false;
		}

		return result;
	}

}
