package com.capstone.frauddetection.hive.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import com.capstone.frauddetection.KafkaTransaction;

public class HiveDAO {

	public static boolean saveCardTransactionsData(KafkaTransaction txn) {

		String sqlStatementInsert = "INSERT INTO TABLE card_transactions_hive(card_id,member_id,amount,postcode,pos_id,transaction_dt,status)"
				+ "VALUES('" + txn.getCard_id() + "','" + txn.getMember_id() + "','" + txn.getAmount() + "','"
				+ txn.getPostcode() + "','" + txn.getPos_id() + "','" + txn.getTransaction_dt() + "','"
				+ txn.getStatus() + "')";

		System.out.println("Sql Statement Insert ==> " + sqlStatementInsert);
		Connection con = null;

		try {

			con = getHiveConnection();
			Statement stmt = con.createStatement();
			stmt.execute(sqlStatementInsert);

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	private static Connection getHiveConnection() {
		Connection con = null;
		try {

			String connectionUrl = "jdbc:hive2://quickstart.cloudera:10000/;ssl=false";
			String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(connectionUrl, "hdfs", "");

			return con;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return con;
	}

}
