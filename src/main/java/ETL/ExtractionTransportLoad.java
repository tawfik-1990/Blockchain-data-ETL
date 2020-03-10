package ETL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.api.java.function.MapFunction;
import static org.apache.spark.sql.functions.row_number;
public class ExtractionTransportLoad {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Stru").master("local[*]")
				.config("spark.sql.warehouse.dir", "target/spark-warehouse")

				.getOrCreate();

		
		
		

		/**
		 * 
		 * Load data from mysql server
		 * 
		 */
		

// get transaction data from type4
		 Dataset<Row> edegs = spark.read().format("jdbc")
					.option("url",
							"jdbc:mysql://url:3306/wavesBI?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC")
					.option("dbtable", " (select sender as sender, recipient as recipient, "
							+ "amount as amount, timestamp as timestamp from wavesBI.type_4 where (assetId is null)  ) as t    ")
					.option("user", "user")
					.option("password", "password")
					.option("numPartitions", 2)
					.option("partitionColumn", "timestamp")
					.option("lowerBound", 	1479682800000L)
					.option("upperBound", 1571090400000L )
					.load();
		 
		 edegs.show();
		 
		 
		 Dataset<Row> df = edegs.select(edegs.col("sender")).distinct();
			Dataset<Row> sender = df.withColumn("id_sender", row_number().over(Window.orderBy("sender") ) );

		
			
			Dataset<Row> df1 = edegs.select(edegs.col("recipient")).distinct();
			Dataset<Row> recipient = df1.withColumn("id_recipient", row_number().over(Window.orderBy("recipient") ).$plus(14000000)  );

			
			sender.createOrReplaceTempView("sender_table");
			recipient.createOrReplaceTempView("recipient_table");
			edegs.createOrReplaceTempView("edegs_table");
			
			Dataset<Row> edegs_join = spark.sql("select r.id_recipient,s.id_sender, E.recipient, E.sender,E.amount from edegs_table E inner join recipient_table r on E.recipient= r.recipient inner join sender_table s on E.sender=s.sender ");
			edegs_join .show();
	     	
			sender.repartition(1).write().format("com.databricks.spark.csv").option("header", "true").save("..\\src\\\\main\\java\\com\\master\\SocialNetworkAnalysis\\resources\\sender111");
			
			recipient.repartition(1).write().format("com.databricks.spark.csv").option("header", "true").save("..\\src\\\\main\\java\\com\\master\\SocialNetworkAnalysis\\resources\\recipient111");
	      
			edegs_join.repartition(14).write().format("com.databricks.spark.csv").option("header", "true").save("..\\src\\\\main\\java\\com\\master\\SocialNetworkAnalysis\\resources\\edeg111");
			
		


		 
		 
		 
		 
	
		
		
		Dataset<Row> data = spark.read().format("jdbc")
				.option("url",
						"jdbc:mysql://url:3306/wavesBI?useUnicode="
						+ "true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC")
				.option("dbtable", " (select cast, sender , amountAsset,  priceAsset,   amount,  time1,  recipient,  amount2,  time2   "
						+ "from ( (select cast(name as CHAR) as cast, order1_senderPublicKey as sender ,"
						+ " order1_assetPair_amountAsset as amountAsset, order1_assetPair_priceAsset as priceAsset, "
						+ "order1_amount as  amount, order1_timestamp as time1, order2_senderPublicKey as recipient, order2_amount as amount2,"
						+ " order2_timestamp as time2  "
						+ "from wavesBI.type_7 inner join wavesBI.type_3 on type_7.order1_assetPair_PriceAsset =  type_3.assetid where (cast(name as CHAR)=\"WBTC\") ) "
						+ "UNION (select cast(name as CHAR) as cast,order1_senderPublicKey as sender , order1_assetPair_amountAsset as amountAsset, "
						+ "order1_assetPair_priceAsset as priceAsset, order1_amount as  amount, order1_timestamp as time1, order2_senderPublicKey as recipient,"
						+ " order2_amount as amount2, order2_timestamp as time2  "
						+ " from wavesBI.type_7 inner join wavesBI.type_3 on type_7.order1_assetPair_amountAsset =  type_3.assetid where (cast(name as CHAR)=\"WBTC\") ) )t   ) as t    ")
				.option("user", "user").option("password", "password").option("numPartitions", 3)
				.option("partitionColumn", "time1").option("lowerBound", 	1492003263380L).option("upperBound", 1543932957000L )
				.load();
		
	// load table type_7

		Dataset<Row> type_7 = spark.read().format("jdbc")
				.option("url",
						"jdbc:mysql://url:3306/wavesBI?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC")
				.option("dbtable", " (select  order1_senderPublicKey as sender , order1_assetPair_amountAsset as amountAsset, order1_assetPair_priceAsset as priceAsset, order1_amount as  amount, order1_timestamp as time1, order2_senderPublicKey as recipient, order2_price as price, order2_timestamp as time2  from wavesBI.type_7   " + 
						") as t    ")
				.option("user", "user").option("password", "password").option("numPartitions", 1)
				.option("partitionColumn", "time1").option("lowerBound", 	1492003263380L).option("upperBound",  1543932957000L )
				.load();
		
		

		
		
		
		
		
//		get asstid of WBTC
		
		Dataset<Row> type_3 = spark.read().format("jdbc")
				.option("url",
						"jdbc:mysql://url:3306/wavesBI?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC")
				.option("dbtable", " (select cast(name as CHAR) as name, assetId as assestId  from wavesBI.type_3 where  cast(name as CHAR)=\"WBTC\"  ) as t    ")
				.option("user", "user")
				.option("password", "password")
				.load();
		
		
//		
		
		
	Dataset <Row> join1 = type_7.join(type_3, type_7.col("priceAsset").equalTo(type_3.col("assestId")));
		
	Dataset <Row> join2 = type_7.join(type_3, type_7.col("amountAsset").equalTo(type_3.col("assestId"))); 
	
	Dataset <Row> join1_filter= join1.filter((join1.col("amountAsset").isNull()).or(join1.col("priceAsset").isNull()) );
	Dataset <Row> join2_filter= join2.filter((join2.col("amountAsset").isNull()).or(join2.col("priceAsset").isNull()) );
	
	Dataset <Row> join= join1_filter.union(join2_filter);

    join.show();
	Dataset<Row> sender_trading = join.select(join.col("sender")).distinct();
	Dataset<Row> recipient_trading = join.select(join.col("recipient")).distinct();
	recipient_trading= recipient_trading.withColumnRenamed("recipient", "sender") ;
	
	Dataset<Row> 	df2=  sender_trading.union(recipient).distinct();
	
	Dataset<Row> edges_S =df2.withColumn("name", lit(df1.col("sender")));

	
	

	

	Dataset<Row> edges_SE_1= join.select(join.col("sender"),join.col("recipient"),join.col("amount"),join.col("time1") );
	
	Dataset<Row> edges_SE_2= 	edges_SE_1.withColumn("type", lit("WBTC"));
	
	Dataset<Row> edges_RE_1= join.select(join.col("recipient"),join.col("sender"),join.col("price"),join.col("time2") );
	Dataset<Row> edges_RE_2= 	edges_RE_1.withColumn("type", lit("WAVES"));
	
	
	edges_RE_2.show();
	
	
	edges_S.repartition(1).write().format("csv").option("header", "true").save("..\\src\\main\\resources\\node1");
	edges_SE_2.repartition(1).write().format("csv").option("header", "true").save("..\\src\\main\\resources\\edges_SE1");      
	edges_RE_2.write().format("csv").option("header", "true").save("..\\src\\main\\resources\\edges_RE1");

	
	sender_trading.repartition(1).write().format("csv").option("header", "true").save("..\\src\\main\\resources\\sender");
	recipient_trading.repartition(1).write().format("csv").option("header", "true").save("..\\src\\main\\resources\\recipient");
	edges_SE_2.repartition(1).write().format("csv").option("header", "true").save("..\\src\\main\\resources\\edge1");      
	edges_RE_2.repartition(1).write().format("csv").option("header", "true").save("..\\src\\main\\resources\\edge2");    

	

	
	

	
	
		Dataset<Row> edege_TO_MYSQL =edegs.select(edegs.col("sender"),edegs.col("recipient"),edegs.col("amount"),edegs.col("timestamp") );
	   edege_TO_MYSQL.repartition(1).write().format("com.databricks.spark.csv").option("header", "true").save("..\\blockchainAna\\src\\main\\resources\\sender111");
		   
	   edege_TO_MYSQL.write()
	  .format("jdbc")
	  .option("url", "jdbc:mysql://url:3306/waves?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&rewriteBatchedStatements=true")
	  .option("dbtable", "type4_assestid_null")
	  
	  .option ("batchsize", "99000")
	  .option("user", "root")
	  .option("password", "password")
	  .mode("Append")
	  .save();
		
		
	
		
	System.out.print("is work");
	}

}
