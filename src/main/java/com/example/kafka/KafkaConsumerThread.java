package com.example.kafka;

import java.util.List;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;

class KafkaConsumerThread extends Thread {

	//Member Variables
	private MyKafkaConsumerListenerInterface		m_listener		= null;
	private String									m_topic			= null;
	private List<KafkaStream<byte[], byte[]>>		m_streams		= null;


	public KafkaConsumerThread(String topic, MyKafkaConsumerListenerInterface listener, List<KafkaStream<byte[], byte[]>> streams) {
		m_topic		= topic;
		m_listener	= listener;
		m_streams	= streams;
	}


	public void setListener(MyKafkaConsumerListenerInterface listener) {
		m_listener = listener;
	}


	@Override
	public void run() {

		MyLogger.getSharedLogger().logDebug(String.format("[KafkaConsumerThread::run] Started Consumer for Topic %s", m_topic));

		ConsumerIterator<byte[], byte[]> it = null;
		String message = null;

		for (KafkaStream<byte[], byte[]> stream : m_streams) {
			it = stream.iterator();
			/*
			while (it.hasNext()) {


				//Provide the new message to the listener
				m_listener.onNewMessageReceived(m_topic, message);
			}
			 */
			while(true) {
				try {
					if(it.hasNext() == true) {
						message = new String(it.next().message());

						if(m_listener != null) {
							m_listener.onNewMessageReceived(m_topic, message);
						}
					} else {
						//Exit the loop
						break;
					}
				} catch(ConsumerTimeoutException e) {

				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		MyLogger.getSharedLogger().logDebug(String.format("[KafkaConsumerThread::run] Stopped Consumer for Topic %s", m_topic));

		super.run();
	}
}