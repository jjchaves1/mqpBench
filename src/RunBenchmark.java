import mongoDB.mongoBench;
public class RunBenchmark {

	public static void main(String[] args) {
		int queryNum = 1;
		if(args.length > 0){
			queryNum = Integer.parseInt(args[0]);
		}
		// TODO Auto-generated method stub
		mongoBench mBench = new mongoBench();
		mBench.aggregate(queryNum);
	}

}
