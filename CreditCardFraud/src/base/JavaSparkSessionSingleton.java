package base;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

/** Lazily instantiated singleton instance of SparkSession */
public class JavaSparkSessionSingleton {
	private static transient Builder instance = null;

	public static Builder getInstance() {
	return getInstance(new SparkConf().setAppName("JavaSqlNetworkWordCount").setMaster("local[2]").set("spark.executor.memory","1g"));
	}
	public static Builder getInstance(SparkConf sparkConf) {
		if(sparkConf ==  null){
			 sparkConf = new SparkConf().setAppName("JavaSqlNetworkWordCount").setMaster("local[2]").set("spark.executor.memory","1g");
		}
		if (instance == null) {
			instance = SparkSession.builder().config(sparkConf);
		}
		return instance;
	}
}