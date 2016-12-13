package mongoDB;
import java.util.ArrayList;
import java.util.Arrays;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
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

	public void aggregate(int queryNumber){
		// 18,801 total documents in collection
		System.out.print("Number of Documents in Collection: ");
		System.out.println(this.coll.count());
		long startTime, endTime, duration;
		
		switch(queryNumber){
		case 1:
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
			startTime = System.nanoTime();
			AggregateIterable<Document> outputOne = this.coll.aggregate(Arrays.asList(
					groupSum, projectSum));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			
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
			break;
		case 2:
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
			break;
		case 3:
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
			break;
		case 4:
			/* db.mainData.find( {}, { crunchbase_url: 1, blog_url: 1, blog_feed_url: 1, homepage_url: 1} ) */
			
			// Project the four URLs for each company
			// (Crunchbase URL, Homepage URL, Blog URL, Blog Feed URL)
			// using the aggregation pipeline
			startTime = System.nanoTime();
			FindIterable<Document> outputFour = 
					this.coll.find().projection(include("crunchbase_url", 
							"homepage_url", "blog_url", "blog_feed_url"));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			outputFour.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			    }
			});
			System.out.print("#4 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		case 5:
			// Project all companies that were founded after
			// the year 2010 using the aggregation pipeline
			startTime = System.nanoTime();
			FindIterable<Document> outputFive = this.coll.find(gt("founded_year", 2010));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			outputFive.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			    }
			});
			System.out.print("#5 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		}
	}
}
