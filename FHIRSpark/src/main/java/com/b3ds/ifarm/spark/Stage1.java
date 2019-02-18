package com.b3ds.ifarm.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class Stage1 {
	private final static String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
	private static String MYSQL_URL = "jdbc:mysql://";
	private static String MYSQL_USER;
	private static String MYSQL_PASSWORD;
	private static String MYSQL_DB;
	private static String MYSQL_STAGE2_DB;
	
	private static SQLContext sql;
	
	private static void checkArgs(String args[]) throws Exception
	{
		if(args.length < 5)
		{
			System.out.println("Usage :- java -jar --db-url=hostname:port --db-user=username --db-password=password --stage1-dbname=dbname1 --stage2-dbname=dbname2");
			System.out.println("Example :- ");
			System.out.println("Example :- java -jar --db-url=localhost:3306 --db-user=root --db-password=123456 --stage1-dbname=stage1 --stage2-dbname=stage2");
			throw new Exception("Please provide all arguments", new Throwable("Missing required arguments"));
		}
	}

	private static String getArgs(String str)
	{
		return str.substring(str.indexOf("=")+1);
	}

	public static void main(String[] args) throws Exception {
		Logger.getLogger("com.b3ds.ifarm.spark").setLevel(Level.ERROR);
		
		checkArgs(args);
		
		MYSQL_URL = MYSQL_URL+getArgs(args[0])+"/";
		MYSQL_USER = getArgs(args[1]);
		MYSQL_PASSWORD = getArgs(args[2]);
		MYSQL_DB = getArgs(args[3]);
		MYSQL_STAGE2_DB = getArgs(args[4]);
		
		SparkConf conf = new SparkConf()
				.setAppName("Demo")
				.setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);		
		sql = new SQLContext(sc);
		
		SparkSession spark = sql.sparkSession();
		
		Dataset<Row> allTables = sql.read()
				.format("jdbc")
				.option("url", MYSQL_URL)
				.option("driver", MYSQL_DRIVER)
				.option("dbtable", "information_schema.tables")
				.option("user", MYSQL_USER)
				.option("password", MYSQL_PASSWORD)
				.load()
				.select("table_name")
				.filter("table_schema = '"+MYSQL_DB+"'");
		
		allTables.foreach(f -> fetchFromTables(f.getString(0)));
//		fetchFromTables("DiagnosticReport");
		allTables.unpersist();
	}

	private static void fetchFromTables(String tablename)
	{
		Dataset<Row> ds = sql.read()
				.format("jdbc")
				.option("url", MYSQL_URL+MYSQL_DB)
				.option("driver", MYSQL_DRIVER)
				.option("dbtable", "`"+tablename+"`")
				.option("user", MYSQL_USER)
				.option("password", MYSQL_PASSWORD)
				.load();
		stageData(ds, tablename);	
		ds.unpersist();
	}
	
	private static void stageData(Dataset<Row> ds, String tablename)
	{
		ds.write()
				.format("jdbc")
				.option("url", MYSQL_URL+MYSQL_STAGE2_DB)
				.option("driver", MYSQL_DRIVER)
				.option("dbtable", "`"+tablename+"`")
				.option("user", MYSQL_USER)
				.option("password", MYSQL_PASSWORD)
				.save();
		ds.unpersist();
	}
}
