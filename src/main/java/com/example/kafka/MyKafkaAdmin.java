package com.example.kafka;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.errors.TopicExistsException;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

//This class implements some of Kafka administration routines (add / remove topics)
public class MyKafkaAdmin {

	//Singleton instance
	private static MyKafkaAdmin				g_sharedKafkaAdmin		= null;

	
	//Member variables
	private ZkClient 							m_zkClient 					= null;
    private ZkUtils 							m_zkUtils 					= null;
    private String								m_zookeeperConnect			= null;
    private int									m_zookeeperID				= 0;

	//Shared constructor
	public static MyKafkaAdmin getInstance(String zookeeperConnect) {

		if(g_sharedKafkaAdmin == null) {
			synchronized(MyKafkaAdmin.class) {
				if(g_sharedKafkaAdmin == null) {
					g_sharedKafkaAdmin = new MyKafkaAdmin(zookeeperConnect);
				}
			}
		}

		return g_sharedKafkaAdmin;
	}


	private MyKafkaAdmin(String zookeeperConnect) {
		
		//Create zookeeper client
		m_zookeeperConnect = zookeeperConnect;
		createZookeeperClient(m_zookeeperConnect);
	}
	
	
	//Create zookeeper client
		private boolean createZookeeperClient(String zookeeperConnect) {
			MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaAdmin::createZookeeperClient]"));
			
			boolean result = true;
			
	        try {
	            String zookeeperHosts = zookeeperConnect; // If multiple zookeeper then -> String zookeeperHosts = "192.168.20.1:2181,192.168.20.2:2181";
	            int sessionTimeOutInMs = 15 * 1000; // 15 secs
	            int connectionTimeOutInMs = 10 * 1000; // 10 secs

	            m_zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
	            m_zkUtils = new ZkUtils(m_zkClient, new ZkConnection(zookeeperHosts), false);
	            
	            //Create new sequence id for the client
	            m_zookeeperID = m_zkUtils.getSequenceId("/" + GlobalConfig.getInstance().getKafkaConsumerGroup(), null);
	            MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaAdmin::createZookeeperClient] Zookeeper ID: %d", m_zookeeperID));


	        } catch (Exception ex) {
	        	MyLogger.getSharedLogger().logError(String.format("[MyKafkaAdmin::createZookeeperClient] Error: %s", ex.getMessage()));
				result = false;
	        }
	        
	        return result;
		}
		
		//Close zookeeper client
		public void closeZookeeperClient() {
			MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaAdmin::closeZookeeperClient]"));
			
			if (m_zkClient != null) {
	            m_zkClient.close();
	        }
		}
		
		
		public int getZookeeperNodeID() {
			return m_zookeeperID;
		}
		
		//create topic
		public boolean createTopic(String topic) {
			MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaAdmin::createTopic]"));
			
			boolean result = true;
	        int noOfPartitions = 1;
	        int noOfReplication = 1;
	        Properties topicConfiguration = new Properties();
	        
	        try {
	        	AdminUtils.createTopic(m_zkUtils, topic, noOfPartitions, noOfReplication, topicConfiguration, RackAwareMode.Disabled$.MODULE$);
	        }
	        catch (TopicExistsException ex) {
	        	MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaAdmin::createTopic] Topic %s already exists", topic));
	        }
	        catch (Exception ex) {
	        	MyLogger.getSharedLogger().logError(String.format("[MyKafkaAdmin::createTopic] Error: %s", ex.getMessage()));
				result = false;
	        }
	        
	        return result;
		}
		
		
		//delete topic
		public boolean deleteTopic(String topic) {
			MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaAdmin::deleteTopic]"));

			boolean result = true;

			if(AdminUtils.topicExists(m_zkUtils, topic)) {
				
				AdminUtils.deleteTopic(m_zkUtils, topic);
				
				MyLogger.getSharedLogger().logDebug(String.format("[MyKafkaAdmin::deleteTopic] Topic %s has been deleted", topic));
				
			} else {
				MyLogger.getSharedLogger().logWarn(String.format("[MyKafkaAdmin::deleteTopic] Topic %s does not exist", topic));
				result = false;
			}

			return result;
		}
		
}
