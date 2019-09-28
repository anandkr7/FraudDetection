package com.capstone.frauddetection;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.capstone.frauddetection.hbase.util.HbaseDAO;

/**
 * Drive class for the Spark Kafka Streaming Consumer and processing of the
 * received card transaction data.
 *
 */
public class KafkaSparkStreamingApplication {

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// Load the zip code csv file from the specified path as argument
		String zipCodeCVS = args[0];

		// Using the random number for the group id
		int random = (int) (Math.random() * 1000 + 1);

		// Configuring the parameters for the Kafka consumer to read the data from
		// respective server
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "100.24.223.181:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "Anand_Kafka_Spark" + random);
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);

		// Configuring the kafka topic to read the card transactions from.
		Collection<String> topics = Arrays.asList("transactions-topic-verified");

		// Creating the Spark configuration
		SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkStreamingDemo").setMaster("local");

		// Creating the Streaming context using spark configuration details.
		// Setting the Spark streaming micro batch window for 1 seconds
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		// Initialize the Hbase Connection for Hbase table operations for both card
		// transactions table and lookup table
		HbaseDAO.getHbaseLookupTableConfig();
		HbaseDAO.getHbaseCardTransactionsTableConfig();

		// Creating the streams from the Java Spark Streaming context
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		// Read the DStream from the received data for processing
		JavaDStream<String> jds = stream.map(x -> x.value());
		jds.foreachRDD(x -> System.out.println("Total Transactions count -  " + x.count()));

		jds.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> rdd) {
				rdd.foreach(txnData -> KafkaSparkService.validateCardTransaction(txnData, zipCodeCVS));
			}
		});

		jssc.start();
		jssc.awaitTermination();

	}

}