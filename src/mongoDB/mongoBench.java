package mongoDB;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.bson.Document;

import com.mongodb.AggregationOutput;
import com.mongodb.DBObject;
/*
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
*/
import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.BasicDBObject;
//import org.bson.Document;

public class mongoBench {
	private MongoClient mongoClient;
	private DB db;
	private DBCollection coll;
	
	public mongoBench(){
		try {
			mongoClient = new MongoClient();
			db = mongoClient.getDB( "mqpBench" );
			coll = db.getCollection( "mainData" );
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
	
	public void insert() {
		/*
		JSONParser parser = new JSONParser();
		try {
			
			int numCompanies = 0;
			JSONArray a = (JSONArray) parser.parse(new FileReader("companies.json"));
			for(Object o : a){
				JSONObject company = (JSONObject) o;
				numCompanies++;
			}
			
			System.out.print("Number of Companies: ");
			System.out.println(numCompanies);
			
		} catch (FileNotFoundException e){
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		} catch (ParseException e){
			e.printStackTrace();
		}*/
		
	}

	public void aggregate(){
		// 18,801 total documents in collection
		System.out.print("Number of Documents in Collection: ");
		System.out.println(this.coll.count());
		
		long startTime = System.nanoTime();
		// Sum the total number of employees across all companies
		// using a full scan
		int sumEmployees = 0;
		BasicDBObject query = new BasicDBObject();
		BasicDBObject field = new BasicDBObject();
		field.put("number_of_employees", 1);
		DBCursor cursor = this.coll.find(query, field);
		System.out.println(cursor.next());
		while (cursor.hasNext()){ 
			BasicDBObject obj = (BasicDBObject) cursor.next();
			String num = obj.getString("number_of_employees");
			if (num != null)
				sumEmployees += Integer.parseInt(num);
		}
		long endTime = System.nanoTime();
		System.out.print("Total Number of Employees: ");
		System.out.println(sumEmployees);
		long duration = (endTime - startTime) / 1000000;
		System.out.print("Time Elapsed: ");
		System.out.print(duration);
		System.out.println("ms.");
		System.out.println();
		
		BasicDBObject objectOne = new BasicDBObject("sum", new BasicDBObject("$sum", "$number_of_employees"));
		BasicDBObject empty = new BasicDBObject();
		BasicDBObject nullID = new BasicDBObject("_id", "null");
		/*
		Iterable<DBObject> output = coll.aggregate(Arrays.asList((DBObject) objectOne )).results();
		for (DBObject dbObject : output){
	        System.out.println(dbObject);
	    } 
		*/
		
		AggregationOutput outputOne = this.coll.aggregate(Arrays.asList(
				(DBObject) new BasicDBObject("$group", nullID.append("sum", objectOne))));
				
	}
}
