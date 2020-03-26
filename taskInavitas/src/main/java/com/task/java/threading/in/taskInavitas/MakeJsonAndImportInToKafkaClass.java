package com.task.java.threading.in.taskInavitas;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.task.java.threading.in.taskInavitas.kafkaconnection.GetKafkaConnection;

/**
 * 
 * @author Prabhakar Kumar Ojha
 *
 */
public class MakeJsonAndImportInToKafkaClass implements FunctionInterface {
	public static Properties instance;
	public static final String TopicName = "sample-data";

	/**
	 * Down side function is responsible to make data as a JsonObject which is
	 * coming from different-2 threads at a time along with that i will call another
	 * function from this function to import data into kafka topic.
	 * 
	 */
	@Override
	public void functionWhichWillMakeJsonObject(String DeviceId, String DataTime, String CurrentA, String CurrentB)
			throws ParseException {
		// TODO Auto-generated method stub
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			JsonObject finalJson = new JsonObject();
			finalJson.addProperty("deviceId", Integer.valueOf(DeviceId.trim()));
			finalJson.addProperty("dataTime", sdf.parse(DataTime).getTime());
			JsonObject jsonA = new JsonObject();
			jsonA.addProperty("dataName", "CurrentA");
			jsonA.addProperty("dataValue", Float.valueOf(CurrentA));
			JsonObject jsonB = new JsonObject();
			jsonB.addProperty("dataName", "CurrentB");
			jsonB.addProperty("dataValue", Float.valueOf(CurrentB));
			JsonArray array = new JsonArray();
			array.add(jsonA);
			array.add(jsonB);

			finalJson.add("dataList", array);

			/**
			 * This function is responsible to get jsonObject from this function
			 * functionWhichWillMakeJsonObject and import it to kafka server
			 */
			importIntoKafkaBroker(finalJson.toString());
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	@Override
	public void importIntoKafkaBroker(String JsonObject) {
		// TODO Auto-generated method stub
		/**
		 * here i am getting Kafka server Connection instance from GetKafkaConnection
		 * class that class will return us the Properties instance which we will pass to
		 * kafka producer
		 */
		instance = GetKafkaConnection.GetConnectionInstanceOfKafka();

		Producer<String, String> producer = new KafkaProducer<>(instance); // Producer object which will take
																			// constructor argument and we need to
																			// specify which types of key value we will
																			// pass
		producer.send(new ProducerRecord<String, String>(TopicName, JsonObject, JsonObject)); // sending the message
		producer.close(); // producer closed
		System.out.println("Message: " + JsonObject + " sent to Topic: " + TopicName); // printing the message that data
																						// inserted to kafka in this
																						// topic

	}

}
