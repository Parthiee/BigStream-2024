package com.bigstream_2024;


import java.util.ArrayList;
import java.util.concurrent.TimeoutException;
import java.util.List;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;


public class SparkStructuredStreamingWithKafka {
	public static void main(String[] args) throws TimeoutException, StreamingQueryException {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkStructuredStreamingWithKafka");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

        List<StructField> fields = new ArrayList<StructField>();
		
		fields.add(DataTypes.createStructField("userID", DataTypes.IntegerType, true));

		fields.add(DataTypes.createStructField("videoID", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("globalTimestamp", DataTypes.TimestampType, true));
		fields.add(DataTypes.createStructField("timestampInVideo", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("action", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("videoType", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("genre",  DataTypes.StringType, true));
		StructType structType = DataTypes.createStructType(fields);

		Dataset<Row> df = spark
				.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "sample")
				.load();

            Dataset<Row> res = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .withColumn("value", from_json(col("value"), structType))
                .select(col("value.*"));

                res.createOrReplaceTempView("video_stream");
		
        spark.sql("SELECT genre, COUNT(genre) AS count FROM video_stream GROUP BY genre")
		.writeStream()
		.format("console")
		.outputMode(OutputMode.Complete())
		.start();
		

        spark.sql("SELECT userID, AVG(timestampInVideo) AS Average_Time FROM video_stream GROUP BY userID")
		.writeStream()
		.format("console")
		.outputMode(OutputMode.Complete())
		.start();

        spark.sql("SELECT videoID, timestampInVideo, frequency FROM (SELECT videoID, timestampInVideo, COUNT(timestampInVideo) AS frequency FROM video_stream WHERE timestampInVideo != 0 GROUP BY videoID, timestampInVideo) AS freq_table ORDER BY frequency DESC")
		.writeStream()
		.format("console")
		.outputMode(OutputMode.Complete())
		.start()
		.awaitTermination();

	}
}