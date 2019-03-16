package com.example.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;


public class GlobalConfig {



	private static GlobalConfig g_sharedGlobalConfig 			= null;
	private String CONFIG_DIR									= "config";
	private String CONFIG_FILE_NAME								= "TransactionManager.config";
	private String PROP_ZOOKEEPER_CONNECT						= "ZOOKEEPER_CONNECT";
	private String PROP_KAFKA_CONSUMER_GROUP					= "KAFKA_CONSUMER_GROUP";
	private String PROP_KAFKA_CONNECT							= "KAFKA_CONNECT";

	private String m_zookeeperConnect							= "localhost:2181";
	private String m_kafkaConsumerGroup							= "MyGROUP";
	private String m_kafkaConnect								= "localhost:9092";
	
	private String m_appPath									= null;


	public static GlobalConfig getInstance() {
		if(g_sharedGlobalConfig == null) {
			g_sharedGlobalConfig = new GlobalConfig();
		}
		return g_sharedGlobalConfig;
	}


	private GlobalConfig() {
		loadConfiguration();
		
		m_appPath = getApplicationPath();
	}


	public void loadConfiguration() {
		File configFile = new File(CONFIG_DIR + "/" + CONFIG_FILE_NAME);
		Properties props = new Properties();
		FileOutputStream outstream = null;
		FileInputStream instream = null;

		//Create the default config file if it does not exist
		if(configFile.exists() == false) {
			try {
				configFile.createNewFile();

				props.setProperty(PROP_ZOOKEEPER_CONNECT, m_zookeeperConnect);
				props.setProperty(PROP_KAFKA_CONSUMER_GROUP, m_kafkaConsumerGroup);
				props.setProperty(PROP_KAFKA_CONNECT, m_kafkaConnect);

				try {
					outstream = new FileOutputStream(configFile);
					props.storeToXML(outstream, null);
				} catch (FileNotFoundException e) {
					MyLogger.getSharedLogger().logError(String.format("[GlobalConfig::loadDefaults] Failed to open file %s. Error: %s", configFile, e.getMessage()));
				}

			} catch (IOException e) {
				MyLogger.getSharedLogger().logError(String.format("[GlobalConfig::loadDefaults] Failed to create file %s. Error: %s", configFile, e.getMessage()));
			}

		} else {	//Load defaults
			try {
				instream = new FileInputStream(configFile);

				props.loadFromXML(instream);

				//Load properties
				m_zookeeperConnect		= props.getProperty(PROP_ZOOKEEPER_CONNECT);
				m_kafkaConsumerGroup	= props.getProperty(PROP_KAFKA_CONSUMER_GROUP);
				m_kafkaConnect			= props.getProperty(PROP_KAFKA_CONNECT);

			} catch (FileNotFoundException e) {
				MyLogger.getSharedLogger().logError(String.format("[GlobalConfig::loadDefaults] File not found %s. Error: %s", configFile, e.getMessage()));
			} catch (InvalidPropertiesFormatException e) {
				MyLogger.getSharedLogger().logError(String.format("[GlobalConfig::loadDefaults] Invalid Property File. Error: %s", e.getMessage()));
			} catch (IOException e) {
				MyLogger.getSharedLogger().logError(String.format("[GlobalConfig::loadDefaults] IO Error for file %s. Error: %s", configFile, e.getMessage()));
			}
		}

		//Close streams
		if(outstream != null) {
			try {
				outstream.close();
			} catch (IOException e) {
				MyLogger.getSharedLogger().logError(String.format("[GlobalConfig::loadDefaults] Failed to close OutputStream. Error: %s", e.getMessage()));
			}
		}

		if(instream != null) {
			try {
				instream.close();
			} catch (IOException e) {
				MyLogger.getSharedLogger().logError(String.format("[GlobalConfig::loadDefaults] Failed to close InputStream. Error: %s", e.getMessage()));
			}
		}
	}

	private static String getApplicationPath() {

		String path = null;
		try {
			path = new File(".").getCanonicalPath();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return path;
	}


	public String getZookeeperConnect() {
		return m_zookeeperConnect;
	}

	public String getKafkaConsumerGroup() {
		return m_kafkaConsumerGroup;
	}

	public String getKafkaConnect() {
		return m_kafkaConnect;
	}

	public String getConfigFileName() {
		return CONFIG_FILE_NAME;
	}
	
	public String getAppPath() {
		return m_appPath;
	}

}
