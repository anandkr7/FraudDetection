package com.capstone.frauddetection.hive.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import com.capstone.frauddetection.KafkaTransaction;

public class HiveDAO {
	
	private static Connection con = null;

	public static boolean saveCardTransactionsData(KafkaTransaction txn) {

		String sqlStatementInsert = "INSERT INTO TABLE card_transactions_hive(card_id,member_id,amount,postcode,pos_id,transaction_dt,status)"
				+ "VALUES('" + txn.getCard_id() + "','" + txn.getMember_id() + "','" + txn.getAmount() + "','"
				+ txn.getPostcode() + "','" + txn.getPos_id() + "','" + txn.getTransaction_dt() + "','"
				+ txn.getStatus() + "')";

		System.out.println("Sql Statement Insert ==> " + sqlStatementInsert);

		try {
			System.out.println("Hive Dao Connection - " + con);
			if(con == null) {
				getHiveConnection();
			}
			Statement stmt = con.createStatement();
			stmt.execute(sqlStatementInsert);

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
		}
		return true;
	}

	public static void getHiveConnection() {
		
		try {

			String connectionUrl = "jdbc:hive://ip-172-31-23-138.ec2.internal:10000/";
			String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(connectionUrl, "hdfs", "");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
