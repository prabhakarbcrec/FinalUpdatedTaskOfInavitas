package com.task.java.threading.in.taskInavitas;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.opencsv.CSVReader;

/**
 * 
 * @author Prabhakar Kumar Ojha
 *
 */

public class CrunchifyThread {
	/**
	 * I have Taken nested List which is $nestedlist
	 */

	public static List<List<String>> nestedlist = new ArrayList<>();
	public static Set<String> searchKey = new HashSet<>();
	public static List<String> keylist = new ArrayList<>();
	public static int threadnumber; // this will count number of thread which will generate on the basis of Distinct
									// DeviceId
	public static int countRowToMachingWitheCsvFile = 0;

	/**
	 * this is for count the row and make sure that whatever rows in there in csv
	 * file that same number if row should go to Kafka
	 */

	public static void main(String args[]) throws InterruptedException, IOException {
		System.out.println("Start-Here");
		String strFile = "src/main/java/SampleData_.csv"; // this is a path of csv file which is available within a
															// project
		CSVReader reader = new CSVReader(new FileReader(strFile)); // Reading the csv file using OpenCsv

		String[] nextLine;
		while ((nextLine = reader.readNext()) != null) {
			List<String> list = new ArrayList<String>(); // so here i am taking all those value inside a list which i
															// need to import into kafka

			list.add(nextLine[0]); // this is a position of DeviceId which i am adding in list at 0 index so on
									// ....
			list.add(nextLine[1]);
			list.add(nextLine[2]);
			list.add(nextLine[3]);
			nestedlist.add(list); // finally list adding into another list which will be like a matrix
			searchKey.add(nextLine[0]); // along with that adding DeviceId into Set<String> for get Distinct number of
										// DeviceId to create number of thread
			// why set<String> -> because Set will not contains duplicate value

		}
		for (String key : searchKey) {
			keylist.add(key);
		}

		/**
		 * here all thread will generate one by on on the basis of DeviceId which is in
		 * keyList that i am passing as a thread name
		 */

		for (threadnumber = 0; threadnumber < keylist.size(); threadnumber++) {

			new ThreadTest("" + keylist.get(threadnumber)).start();

		}

	}
}

class ThreadTest extends Thread {
	/**
	 * extending thread class here to get run method
	 */
	String threadingmatchingkey;
	MakeJsonAndImportInToKafkaClass ObjectToCallFunction = new MakeJsonAndImportInToKafkaClass();

	public ThreadTest(String str) {
		super(str);
		threadingmatchingkey = str;
	}

	public void run() {
		/**
		 * Loop will run till number of row, if will check Thread name and
		 * RowId(DeviceId ) should me equals because i have initialized one-2 thread to
		 * one-2 DeviceId
		 */
		for (int row = 0; row < CrunchifyThread.nestedlist.size(); row++) {
			if (CrunchifyThread.nestedlist.get(row).get(0).equals(threadingmatchingkey)) {
				CrunchifyThread.countRowToMachingWitheCsvFile++;

				/**
				 * if want to verify whether all row is coming or not, how thread is running
				 * concurrently you can uncomment below S.o.p and check it.
				 */

//				System.out.println("row: " + CrunchifyThread.countRowToMachingWitheCsvFile + "  " + "ThreadName:  "
//						+ getName() + "  " + CrunchifyThread.nestedlist.get(row).get(0) + " "
//						+ CrunchifyThread.nestedlist.get(row).get(1) + "    "
//						+ CrunchifyThread.nestedlist.get(row).get(2) + "  "
//						+ CrunchifyThread.nestedlist.get(row).get(3));

				try {
					/**
					 * Here i am going to pass all required data to make a JsonObject that function
					 * name is functionWhichWillMakeJsonObject which is locating in
					 * MakeJsonAndImportInToKafkaClass class
					 * 
					 */

					ObjectToCallFunction.functionWhichWillMakeJsonObject(CrunchifyThread.nestedlist.get(row).get(0),
							CrunchifyThread.nestedlist.get(row).get(1), CrunchifyThread.nestedlist.get(row).get(2),
							CrunchifyThread.nestedlist.get(row).get(3));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}