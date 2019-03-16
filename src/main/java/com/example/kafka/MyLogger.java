package com.example.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class MyLogger {
	
	//Shared Logger Instance
	private static MyLogger g_sharedLogger = null;
	
	private Logger m_logger = null;
	private static Object syncObj = new Object();
	
	private enum LogLevel {
		INFO,
		DEBUG,
		ERROR,
		WARN
	}
	
	private MyLogger() {
		//Initialize log4j Logger
		m_logger = Logger.getLogger("kafka-example");
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);      
		Logger.getLogger("kafka").setLevel(Level.OFF);
		PropertyConfigurator.configure("log4j.properties");
	}
	
	public void logDebug(String message) {
		log(LogLevel.DEBUG, message);
	}
	
	public void logInfo(String message) {
		log(LogLevel.INFO, message);
	}
	
	public void logWarn(String message) {
		log(LogLevel.WARN, message);
	}

	public void logError(String message) {
		log(LogLevel.ERROR, message);
	}

	public void log(LogLevel level, String message) {
		synchronized (syncObj) {

			switch(level) {
			case DEBUG:
				m_logger.debug(message);
				break;
			case INFO:
				m_logger.info(message);
				break;
			case WARN:
				m_logger.warn(message);
				break;
			case ERROR:
				m_logger.error(message);
				break;
			default:
				m_logger.debug(message);
			}
		}
	}
	
	static public MyLogger getSharedLogger() {
		synchronized(syncObj) {
			if (g_sharedLogger == null) {
				g_sharedLogger = new MyLogger();
			}
		}
		
		return g_sharedLogger;
	}
}
