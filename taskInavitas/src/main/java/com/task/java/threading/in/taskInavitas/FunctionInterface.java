package com.task.java.threading.in.taskInavitas;

import java.text.ParseException;

/**
 * 
 * @author Prabhakar Kumar Ojha
 *
 *         Here I am declaring all the function inside this interface which will
 *         use.
 */

public interface FunctionInterface {
	public void functionWhichWillMakeJsonObject(String DeviceID, String DataTime, String CurrentA, String CurrentB)
			throws ParseException;

	public void importIntoKafkaBroker(String JsonObject);

}
