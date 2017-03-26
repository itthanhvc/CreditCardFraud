package sparkSQL;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataService {
	private static transient DataService instance = null;
	// create singleton for DataService
	public static DataService getInstance(SparkContext ct) {
		if(instance == null)
			instance = new DataService(ct);
		return instance;
	}
	// Store SparkSession
	private SparkSession spark;

	private DataService(SparkContext ct) {
		SparkConf sparkConf = new SparkConf()
						.setMaster("local[2]")
						.set("hive.metastore.warehouse.dir", "file:/user/hive/warehouse")
						.set("hive.metastore.uris", "thrift://127.0.0.1:9083");

		spark = SparkSession
				.builder()
				.config(sparkConf)
				.enableHiveSupport()
				.getOrCreate();

		// Create if not exist for Account Table
		spark.sql("CREATE TABLE IF NOT EXISTS Account (key INT, AccountNo STRING, User STRING, HomeAddress STRING, AccountType STRING)");
		// Create if not exist for Transaction Table
		spark.sql("CREATE TABLE IF NOT EXISTS Transaction (TransactionId INT, AccountNo STRING, Time STRING, Location STRING, Amount FLOAT, Alert STRING)");
	}

	public Account getAccount(String accNo) {
		Dataset<Row> account = spark
				.sql("select * from Account where AccountNo='" + accNo + "'");
		
		Iterator<Row> rs= account.toLocalIterator();
		Account result = null;
		if(rs.hasNext()){
			Row arg0 = rs.next();
			String accNo1 = arg0.getString(arg0.fieldIndex("AccountNo"));
			String user = arg0.getString(arg0.fieldIndex("User"));
			String homeAddress = arg0.getString(arg0.fieldIndex("HomeAddress"));
			String accountType = arg0.getString(arg0.fieldIndex("AccountType"));
			result = new Account(accNo1, user, homeAddress, accountType);
		}
		return result;
	}
	
	// Get the most 24 transactions of User based on Account NO
	public List<Transaction> getRecentTransactions(String AccountNo) {
		Dataset<Row> transactions = spark
				.sql("select * "
				+ "from Transaction " + "where Alert <> 'Fraud' and AccountNo = '"
				+ AccountNo + "' " + "Limit 24");		
		Iterator<Row> rs= transactions.toLocalIterator();
		List<Transaction> result=  new ArrayList<Transaction>();
		while(rs.hasNext()){
			Row arg0 = rs.next();
			int transId = arg0.getInt(arg0.fieldIndex("TransactionId"));
			String accNo = arg0.getString(arg0.fieldIndex("AccountNo"));
			String transactionTime = arg0.getString(arg0.fieldIndex("Time"));
			String location = arg0.getString(arg0.fieldIndex("Location"));
			float amount = arg0.getFloat(arg0.fieldIndex("Amount"));
			String alert = arg0.getString(arg0.fieldIndex("Alert"));
			result.add(new Transaction(transId, accNo, transactionTime,
					location, amount, alert));
		}
		return result;
	}
	
	public void insertNewTransaction(JavaRDD<Transaction> rdd) throws AnalysisException{
		Dataset<Row> record = spark.createDataFrame(rdd, Transaction.class);
		
		record.createOrReplaceTempView("currentTrans");
		
		Dataset<Row> df = spark
		.sql("select transactionid, accountno, time, location, amount, alert from currentTrans");
		df.show();
		df
		 .write()
		 .format("orc")
		 .mode(SaveMode.Append)
		 .insertInto("default.transaction");
	}
}
