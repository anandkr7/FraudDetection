package com.capstone.frauddetection;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

import com.google.gson.Gson;

public class KafkaSparkStreaming {

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkConf sparkConf = new SparkConf().setAppName(
				"KafkaSparkStreamingDemo").setMaster("local");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(1));

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "100.24.223.181:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "Anand_Kafka_Spark14");
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);

		Collection<String> topics = Arrays
				.asList("transactions-topic-verified");

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
				.createDirectStream(jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String> Subscribe(topics,
								kafkaParams));

		JavaDStream<String> jds = stream.map(x -> x.value());
		jds.foreachRDD(x -> System.out.println(x.count()));

		jds.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> rdd) {
				rdd.foreach(a -> validateCardTransaction(a));
			}
		});

		jssc.start();
		jssc.awaitTermination();

	}

	public static String validateCardTransaction(String data) {

		String postCode = "";
		String transactionDt = "";
		
		try {
			if (data.length() > 10) {
				Gson gson = new Gson();
				KafkaTransaction txn = gson.fromJson(data,
						KafkaTransaction.class);
				int memberScore = HbaseDAO.getScore(new TransactionData(txn
						.getCard_id()));
				Double uclValue = HbaseDAO
						.getUCLForTransaction(new TransactionData(txn
								.getCard_id()));
				postCode = HbaseDAO
						.getPostCodeForTransaction(new TransactionData(txn
								.getCard_id()));
				transactionDt = HbaseDAO
						.getTxnTimeForTransaction(new TransactionData(txn
								.getCard_id()));
				boolean genuineFlag = true;
				if (memberScore > 0) {

					if (Double.valueOf(txn.getAmount()) < new BigDecimal(
							uclValue).doubleValue()) {
						genuineFlag = true;
					} else {
						genuineFlag = false;
					}

					if (memberScore > 200) {
						genuineFlag = true;
					} else {
						genuineFlag = false;
					}

					DistanceUtility distanceUtility = new DistanceUtility();

					Double distance = distanceUtility.getDistanceViaZipCode(
							txn.getPostcode(), postCode);
					Double distancePerSecond = distance
							/ ((getMilliseconds(txn.getTransaction_dt()) - getMilliseconds(transactionDt)) / 1000);
					System.out
							.println("Card Id -- "
									+ txn.getCard_id()
									+ " Distance -- "
									+ new BigDecimal(distance).doubleValue()
									+ " === Distance Per Second -- "
									+ new BigDecimal(distancePerSecond)
											.toPlainString());
					if (distancePerSecond < 0.25) {
						genuineFlag = true;
					} else {
						genuineFlag = false;
					}

					/*
					 * System.out.println("Card Id -- " + txn.getCard_id() +
					 * " Member Id -- " + txn.getMember_id() + "" +
					 * " Member Score -- " + memberScore + " UCL -- " + new
					 * BigDecimal(uclValue).toPlainString() + " Txn Amount -- "
					 * + txn.getAmount() + " Postcode -- " + txn.getPostcode() +
					 * " Genuine -- " + genuineFlag);
					 */

					if (genuineFlag) {
						txn.setStatus("GENUINE");
					} else {
						txn.setStatus("FRAUD");
					}
					
					saveCardTransactionsData(txn);
					if(genuineFlag){
						HbaseDAO.saveHbaseLookupData(txn, postCode, transactionDt);
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return "";
	}

	public static Long getMilliseconds(String date) {
		final String strDate = date;
		Long millis = 0l;
		try {
			millis = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss").parse(strDate)
					.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return millis;
	}

	public static String saveCardTransactionsData(KafkaTransaction txn) {

		Connection con = null;
		String connectionUrl = "jdbc:hive2://quickstart.cloudera:10000/;ssl=false";
		String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

		String sqlStatementInsert = "INSERT INTO TABLE card_transactions_hive(card_id,member_id,amount,postcode,pos_id,transaction_dt,status)"
				+ "VALUES('"
				+ txn.getCard_id()
				+ "','"
				+ txn.getMember_id()
				+ "','"
				+ txn.getAmount()
				+ "','"
				+ txn.getPostcode()
				+ "','"
				+ txn.getPos_id()
				+ "','"
				+ txn.getTransaction_dt()
				+ "','"
				+ txn.getStatus() + "')";

		System.out.println("Sql Statement Insert ==> " + sqlStatementInsert);

		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(connectionUrl, "hdfs", "");
			Statement stmt = con.createStatement();
			stmt.execute(sqlStatementInsert);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return connectionUrl;

	}

}