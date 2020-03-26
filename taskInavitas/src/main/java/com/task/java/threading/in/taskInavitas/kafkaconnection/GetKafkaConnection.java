package com.task.java.threading.in.taskInavitas.kafkaconnection;

import java.io.Serializable;
import java.util.Properties;

/**
 * 
 * @author Prabhakar Kumar Ojha
 *
 */
public class GetKafkaConnection implements Serializable {
	/**
	 * This is a kafka connection class, here i am making instance with the help of
	 * Singleton and rerunning back this is king of Design Pattern for making
	 * connection with any server like Database,kafka,Hbase, etc Generate connection
	 * instance once and use many times
	 */
	private static final long serialVersionUID = 1L;
	// static Properties properties;
	public static final int BATCHSIZE = 16384;
	public static final int BUFFERMEMORY = 33554432;
	public static final String BOOTSTRAP_SERVER_NAME_WITH_PORT = "localhost:9092";
	public static final String KEY_SERIALIZATION_CLASS_NAME = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String VALUE_SERIALIZATION_CLASS_NAME = "org.apache.kafka.common.serialization.StringSerializer";

	private GetKafkaConnection() {
		properties = new Properties();
		properties.put("bootstrap.servers", BOOTSTRAP_SERVER_NAME_WITH_PORT);
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", BATCHSIZE);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", BUFFERMEMORY);
		properties.put("key.serializer", KEY_SERIALIZATION_CLASS_NAME);
		properties.put("value.serializer", VALUE_SERIALIZATION_CLASS_NAME);

	}

	private static Properties properties;

	public static Properties GetConnectionInstanceOfKafka() {
		if (properties == null) {
			synchronized (GetKafkaConnection.class) {
				new GetKafkaConnection();
			}

		}

		return properties;
	}
}
