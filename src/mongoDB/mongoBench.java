package mongoDB;
import java.util.ArrayList;
import java.util.Arrays;

import org.bson.BsonValue;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.Block;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Filters.*;

public class mongoBench {
	private MongoClient mongoClient;
	private MongoDatabase db;
	private MongoCollection<Document> coll;
	
	public mongoBench(){
	
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase( "mqpBench" );
		coll = db.getCollection( "mainData" );
	
		
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
		/*
		long startTime = System.nanoTime();
		// Sum the total number of employees across all companies
		// using a full scan
		int sumEmployees = 0;
		Document query = new Document();
		Document field = new Document();
		field.put("number_of_employees", 1);
		FindIterable cursor = this.coll.find(query, field);
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
		*/
		
		/*
		db.mainData.aggregate({ $group: {
									_id: '', 
									sum: { $sum: '$number_of_employees }
										}
								}, 
								{$project: { 
									_id: 0, 
									sum: '$sum' }
								})
		*/
		// Sum the total number of employees across all companies
		// using the aggregation pipeline
		Document sum = new Document("$sum", "$number_of_employees");
		Document idSum = new Document( "_id", "").append("sum", sum);
		Document groupSum = new Document( "$group", idSum);
		Document pidSum = new Document( "_id", "$0").append("sum", "$sum");
		Document projectSum = new Document("$project", pidSum);
		long startTime = System.nanoTime();
		AggregateIterable<Document> outputOne = this.coll.aggregate(Arrays.asList(
				groupSum, projectSum));
		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / 1000000;
		
		outputOne.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document.toJson());
		    }
		});
			
		System.out.print("#1 Time Elapsed: ");
		System.out.print(duration);
		System.out.println("ms.");
		System.out.println();
		
		/* db.mainData.aggregate([{ "$group" : { _id: "$founded_year", count: {$sum:1}}}])  */
		// Group companies by the year they were founded in
		// using the aggregation pipeline
		Document count = new Document("$sum", 1);
		Document id = new Document("_id", "$founded_year");
		Document objectTwo = new Document("$group", id.append("count", count));
		startTime = System.nanoTime();
		AggregateIterable<Document> outputTwo = this.coll.aggregate(Arrays.asList(objectTwo));
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000;
		
		outputTwo.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document.toJson());
		    }
		});
		
		System.out.print("#2 Time Elapsed: ");
		System.out.print(duration);
		System.out.println("ms.");
		System.out.println();
		
		// Project each distinct category code
		// using the aggregation pipeline
		startTime = System.nanoTime();
		ArrayList<String> distinctCategories = 
				this.coll.distinct("category_code", String.class)
					.filter(new Document("category_code",new Document("$ne",null)))
						.into(new ArrayList<String>());;
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000;
		System.out.println("Distinct Category Codes: ");
		for(String cat : distinctCategories){
			System.out.println(cat);
		}
		
		System.out.print("#3 Time Elapsed: ");
		System.out.print(duration);
		System.out.println("ms.");
		System.out.println();
		
		
		/* db.mainData.find( {}, { crunchbase_url: 1, blog_url: 1, blog_feed_url: 1, homepage_url: 1} ) */
		
		// Project the four URLs for each company
		// (Crunchbase URL, Homepage URL, Blog URL, Blog Feed URL)
		// using the aggregation pipeline
		Document cbURL = new Document("_id", "$crunchbase_url");
		startTime = System.nanoTime();
		FindIterable<Document> outputFour = 
				this.coll.find().projection(include("crunchbase_url", 
						"homepage_url", "blog_url", "blog_feed_url"));
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000;
		/*
		outputFour.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document.toJson());
		    }
		});
		*/
		System.out.print("#4 Time Elapsed: ");
		System.out.print(duration);
		System.out.println("ms.");
		System.out.println();
		
		
		// Project all companies that were founded after
		// the year 2010 using the aggregation pipeline
		startTime = System.nanoTime();
		FindIterable<Document> outputFive = this.coll.find(gt("founded_year", 2010));
		endTime = System.nanoTime();
		duration = (endTime - startTime) / 1000000;
		/*
		outputFive.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		        System.out.println(document.toJson());
		    }
		});
		*/
		System.out.print("#5 Time Elapsed: ");
		System.out.print(duration);
		System.out.println("ms.");
		System.out.println();
		
	}
}
