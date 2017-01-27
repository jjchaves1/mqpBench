import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import mongoDB.mongoBench;
public class RunBenchmark {

	public static void main(String[] args) {
		// Default query option
		int queryNum = 1;
		if(args.length > 0){
			// Query to run is taken as a command line argument
			queryNum = Integer.parseInt(args[0]);
		}
		// Create a mongoBench instance
		mongoBench mBench = new mongoBench();
		try {
			// Run query 
			mBench.aggregate(queryNum);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

}
