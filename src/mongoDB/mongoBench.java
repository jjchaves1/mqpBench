package mongoDB;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
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

	public void aggregate(int queryNumber) throws FileNotFoundException, UnsupportedEncodingException{
		// *** MongoDB diagnostics INFO logs will print in red ***
		// 18,801 total documents in collection
		System.out.print("Number of Documents in Collection: ");
		System.out.println(this.coll.count());
		
		// Variables for timing
		long startTime, endTime, duration;
		
		// Switch on the command line argument
		switch(queryNumber){
		case 1:
			// Sum the total number of employees across all companies
			/* NoSQL Command:
			 * db.mainData.aggregate({ $group: {_id: '', 
										sum: { $sum: '$number_of_employees } } }, 
									{$project: {_id: 0, 
												sum: '$sum' } } )
			*/
			
			// Form query with Document objects
			Document sum = new Document("$sum", "$number_of_employees");
			Document idSum = new Document( "_id", "").append("sum", sum);
			Document groupSum = new Document( "$group", idSum);
			Document pidSum = new Document( "_id", "$0").append("sum", "$sum");
			Document projectSum = new Document("$project", pidSum);
			// Run & Time Query 
			startTime = System.nanoTime();
			AggregateIterable<Document> outputOne = this.coll.aggregate(Arrays.asList(
					groupSum, projectSum));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			// Write results to file & print to console
			PrintWriter writer1 = new PrintWriter("Q1Results.txt", "UTF-8");
			outputOne.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			        writer1.println(document.toJson());
			    }
			});
			// Print statistics to console
			System.out.print("#1 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		case 2:
			// Count the number of companies founded in each year
			/* NoSQL Command:
			 * db.mainData.aggregate([{ "$group" : { _id: "$founded_year", count: {$sum:1}}}])  */
			
			// Form query with Document objects
			Document count = new Document("$sum", 1);
			Document id = new Document("_id", "$founded_year");
			Document objectTwo = new Document("$group", id.append("count", count));
			// Run & Time Query 
			startTime = System.nanoTime();
			AggregateIterable<Document> outputTwo = 
					this.coll.aggregate(Arrays.asList(objectTwo));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			// Write results to file & print to console
			PrintWriter writer2 = new PrintWriter("Q2Results.txt", "UTF-8");
			outputTwo.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			        writer2.println(document.toJson());
			    }
			});
			// Print statistics to console
			System.out.print("#2 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		case 3:
			// Project each distinct category code
			
			// Run & Time Query 
			startTime = System.nanoTime();
			ArrayList<String> distinctCategories = 
					this.coll.distinct("category_code", String.class)
						.filter(new Document("category_code",new Document("$ne",null)))
							.into(new ArrayList<String>());
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			// Write results to file & print to console
			PrintWriter writer3 = new PrintWriter("Q3Results.txt", "UTF-8");
			System.out.println("Distinct Category Codes: ");
			for(String cat : distinctCategories){
				System.out.println(cat);
				writer3.println(cat);
			}
			// Print statistics to console
			System.out.print("#3 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		case 4:
			// Project the four URLs for each company
			// (Crunchbase URL, Homepage URL, Blog URL, Blog Feed URL)
			/* NoSQL Command:
			 * db.mainData.find( {}, { crunchbase_url: 1, blog_url: 1, blog_feed_url: 1, homepage_url: 1} ) */
			
			// Run & Time Query 
			startTime = System.nanoTime();
			FindIterable<Document> outputFour = 
					this.coll.find().projection(include("crunchbase_url", 
							"homepage_url", "blog_url", "blog_feed_url"));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			// Write results to file & print to console
			PrintWriter writer4 = new PrintWriter("Q4Results.txt", "UTF-8");
			outputFour.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			        writer4.println(document.toJson());
			    }
			});
			// Print statistics to console
			System.out.print("#4 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		case 5:
			// Project all companies that were founded after the year 2010 
			
			// Run & Time Query 
			startTime = System.nanoTime();
			FindIterable<Document> outputFive = this.coll.find(gt("founded_year", 2010));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			// Write results to file & print to console
			PrintWriter writer5 = new PrintWriter("Q5Results.txt", "UTF-8");
			outputFive.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			        writer5.println(document.toJson());
			    }
			});
			// Print statistics to console
			System.out.print("#5 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
			
		/*** Querying Nested Documents ***/
			
		case 6:
			// Return each distinct acquisition currency code
			/* NoSQL Command:
			 * db.mainData.distinct('acquisition.price_currency_code') */
			
			// Run & Time Query 
			startTime = System.nanoTime();
			ArrayList<String> distinctPriceCurrCode = 
					this.coll.distinct("acquisition.price_currency_code", String.class)
						.into(new ArrayList<String>());		
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			// Write results to file & print to console
			PrintWriter writer6 = new PrintWriter("Q6Results.txt", "UTF-8");
			System.out.println("Distinct Category Codes: ");
			for(String code : distinctPriceCurrCode){
				System.out.println(code);
				writer6.println(code);
			}
			// Print statistics to console
			System.out.print("#6 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		case 7:
			// Return number of offices in each city,
			// Grouped by city name
			/* NoSQL Command:
			 * db.mainData.aggregate([ {"$project": {"offs" : "$offices" }}, 
			 * 		{"$unwind" : "$offs"}, 
			 * 		{$group: {_id: "$offs.city", 
			 * 				  count: { $sum: 1}}}]) */
			
			// Form query with Document objects
			Document offs = new Document("offs", "$offices");
			Document project7 = new Document("$project", offs);
			Document unwind7 = new Document("$unwind", "$offs");
			Document sum7 = new Document("$sum", 1);
			Document id7 = new Document("_id", "$offs.city");
			Document group7 = new Document("$group", id7.append("count", sum7));
			// Run & Time Query 
			startTime = System.nanoTime();
			AggregateIterable<Document> outputSeven = 
					this.coll.aggregate(Arrays.asList(project7, unwind7, group7));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			// Write results to file & print to console
			PrintWriter writer7 = new PrintWriter("Q7Results.txt", "UTF-8");
			outputSeven.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			        writer7.println(document.toJson());
			    }
			});
			// Print statistics to console
			System.out.print("#7 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		case 8:
			// Return each company name,
			// and their smallest available image size
			/* NoSQL Command:
			 * db.mainData.aggregate([ {"$project": 
			 * 		{"pics" : "$image.available_sizes", "compname" : "$name"}}, 
			 * {$unwind: "$pics"}, 
			 * {$group: {"_id": "$compname", "size": {$first: "$pics"}}} ]) */
			
			// Form query with Document objects
			Document projection8 = new Document("pics", "$image.available_sizes")
				.append("compname", "$name");
			Document project8 = new Document("$project", projection8);
			Document unwind8 = new Document("$unwind", "$pics");
			Document first8 = new Document("$first", "$pics");
			Document grouping8 = new Document("_id", "$compname")
				.append("size", first8);
			Document group8 = new Document("$group", grouping8);
			// Run & Time Query 
			startTime = System.nanoTime();
			AggregateIterable<Document> outputEight = 
					this.coll.aggregate(Arrays.asList(project8, unwind8, group8));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			// Write results to file & print to console
			PrintWriter writer8 = new PrintWriter("Q8Results.txt", "UTF-8");
			outputEight.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			        writer8.println(document.toJson());
			    }
			});
			// Print statistics to console
			System.out.print("#8 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		case 9:
			// Return each company name and the total amount 
			// of funding they have raised
			/* NoSQL Command:
			 * db.mainData.aggregate([ {$unwind: "$funding_rounds"}, 
			 * 		{$group: { _id: "$name", 
			 * 					"total_funding": 
			 * 						{$sum: "$funding_rounds.raised_amount"}}}]) */
			
			// Form query with Document objects
			Document unwind9 = new Document("$unwind", "$funding_rounds");
			Document sum9 = new Document("$sum", "$funding_rounds.raised_amount");
			Document id9 = new Document("_id", "$name");
			Document group9 = new Document("$group", id9.append("total_funding", sum9));
			// Run & Time Query 
			startTime = System.nanoTime();
			AggregateIterable<Document> outputNine = 
					this.coll.aggregate(Arrays.asList(unwind9, group9));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			// Write results to file & print to console
			PrintWriter writer9 = new PrintWriter("Q9Results.txt", "UTF-8");
			outputNine.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			        writer9.println(document.toJson());
			    }
			});
			// Print statistics to console
			System.out.print("#9 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		case 10:
			// Project all company records whose acquisition price amount
			// is greater than 2,000,000,000
			
			// Run & Time Query 
			startTime = System.nanoTime();
			FindIterable<Document> outputTen = 
				this.coll.find(gt("acquisition.price_amount", 2000000000));
			endTime = System.nanoTime();
			duration = (endTime - startTime) / 1000000;
			// Write results to file & print to console
			PrintWriter writer10 = new PrintWriter("Q10Results.txt", "UTF-8");
			outputTen.forEach(new Block<Document>() {
			    @Override
			    public void apply(final Document document) {
			        System.out.println(document.toJson());
			        writer10.println(document.toJson());
			    }
			});
			// Print statistics to console
			System.out.print("#10 Time Elapsed: ");
			System.out.print(duration);
			System.out.println("ms.");
			System.out.println();
			break;
		}
	}
}
