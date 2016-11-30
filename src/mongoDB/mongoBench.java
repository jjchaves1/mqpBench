package mongoDB;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
		
		// Project each distinct category code
		// using the aggregation pipeline
		List<String> distinctCategories = this.coll.distinct("category_code");
		System.out.println(distinctCategories);
		
		// Sum the total number of employees across all companies
		// using the aggregation pipeline
		BasicDBObject objectOne = new BasicDBObject("sum", new BasicDBObject("$sum", "$number_of_employees"));
		AggregationOutput outputOne = this.coll.aggregate(Arrays.asList(
				(DBObject) new BasicDBObject("$project", objectOne)));
				
		// Group companies by the year they were founded in
		// using the aggregation pipeline
		BasicDBObject companyName = new BasicDBObject("$company", "$company_name");
		BasicDBObject foundedYear = new BasicDBObject("Year", new BasicDBObject("$year", "$founded_year"));
		BasicDBObject objectTwo = new BasicDBObject("$group", new BasicDBObject("$_id", foundedYear).append("Company", companyName));
		AggregationOutput outputTwo = this.coll.aggregate(Arrays.asList((DBObject) objectTwo));
		
		
		
		// Project the four URLs for each company
		// (Crunchbase URL, Homepage URL, Blog URL, Blog Feed URL)
		// using the aggregation pipeline
		BasicDBObject crunchURL = new BasicDBObject("cruchURL", new BasicDBObject("$crunchURL", "$crunchbase_url"));
		BasicDBObject homepageURL = new BasicDBObject("$homepageURL", "$homepage_url");
		BasicDBObject blogURL = new BasicDBObject("$blogURL", "$blog_url");
		BasicDBObject blogfeedURL = new BasicDBObject("$blogfeedURL", "$blog_feed_url");
		BasicDBObject objectFour = new BasicDBObject("$project", crunchURL.append("homepageURL", homepageURL).append("blogURL", blogURL).append("blogfeedURL", blogfeedURL));
		AggregationOutput outputFour = this.coll.aggregate(Arrays.asList((DBObject) objectFour));
		
		// Project all companies that were founded after
		// the year 2010 using the aggregation pipeline
		BasicDBObject filterInput = new BasicDBObject("$input", "$founded_year");
		BasicDBObject filterCond = new BasicDBObject("gte", Arrays.asList("$$year", 2010));
		BasicDBObject filter = new BasicDBObject("$filter", filterInput.append("as", "year").append("cond", filterCond));
		BasicDBObject filterByYear = new BasicDBObject("founded_after_2010", filter);
		BasicDBObject objectFive = new BasicDBObject("$project", filterByYear);
		AggregationOutput outputFive = this.coll.aggregate(Arrays.asList((DBObject) objectFive));
		
	}
}
