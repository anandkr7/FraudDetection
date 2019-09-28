package com.capstone.frauddetection.hbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.capstone.frauddetection.KafkaTransaction;
import com.capstone.frauddetection.TransactionData;

/**
 * Class for creating Hbase connection and for Hbase table operations a) To get
 * the card transactions and lookup table connection b) To fetch and save
 * operations on the table
 */
public class HbaseDAO {

	private static HTable lookupTable = null;
	private static HTable cardTransactionsTable = null;

	// Method to fetch the member score value from the lookup table for card_id
	public static int getScore(TransactionData transactionData) throws IOException {

		try {
			Get g = new Get(Bytes.toBytes(transactionData.getMemberId()));
			Result result = lookupTable.get(g);
			byte[] value = result.getValue(Bytes.toBytes("col_family"), Bytes.toBytes("score"));
			if (value != null) {
				return Integer.parseInt(Bytes.toString(value));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;

	}

	// Method to fetch the latest UCL value calculated for the card_id
	public static Double getUCLForTransaction(TransactionData transactionData) throws IOException {

		try {
			Get g = new Get(Bytes.toBytes(transactionData.getMemberId()));
			Result result = lookupTable.get(g);
			byte[] value = result.getValue(Bytes.toBytes("col_family"), Bytes.toBytes("UCL"));
			if (value != null) {
				return Double.parseDouble(Bytes.toString(value));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0d;
	}

	// Method to fetch the latest post code value for the card id
	public static String getPostCodeForTransaction(TransactionData transactionData) throws IOException {

		try {
			Get g = new Get(Bytes.toBytes(transactionData.getMemberId()));
			Result result = lookupTable.get(g);
			byte[] value = result.getValue(Bytes.toBytes("col_family"), Bytes.toBytes("postcode"));
			if (value != null) {
				return Bytes.toString(value);
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
		return null;
	}

	// Method to fetch the latest transaction data time for the card id
	public static String getTxnTimeForTransaction(TransactionData transactionData) throws IOException {

		try {

			Get g = new Get(Bytes.toBytes(transactionData.getMemberId()));
			Result result = lookupTable.get(g);
			byte[] value = result.getValue(Bytes.toBytes("col_family"), Bytes.toBytes("transaction_dt"));
			if (value != null) {
				return Bytes.toString(value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	// Method to save the latest postcode and the transaction data for the card id
	// in lookup table if the transaction is categorized as GENUINE
	public static String saveHbaseLookupData(KafkaTransaction txn, String currentPostcode, String currentTxnDt) {

		try {

			Put p = new Put(Bytes.toBytes(txn.getCard_id()));
			// Updating a cell value
			p.addColumn(Bytes.toBytes("col_family"), Bytes.toBytes("postcode"), Bytes.toBytes(txn.getPostcode()));
			p.addColumn(Bytes.toBytes("col_family"), Bytes.toBytes("transaction_dt"),
					Bytes.toBytes(txn.getTransaction_dt()));

			// Saving the put Instance to the HTable.
			lookupTable.put(p);
			System.out.println("Data Updated -- Current PostCode - " + currentPostcode + " --  Updated Postcode - "
					+ txn.getPostcode() + " %%%%% Current Date - " + currentTxnDt + " -- Updated Date - "
					+ txn.getTransaction_dt());

		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	// Method to save the card transactions details in the card transaction table
	public static String saveCardTransactionsHbase(KafkaTransaction txn) {

		try {

			Put p = new Put(Bytes.toBytes(txn.getCard_id()));
			p.addColumn(Bytes.toBytes("col_family"), Bytes.toBytes("card_id"), Bytes.toBytes(txn.getCard_id()));
			p.addColumn(Bytes.toBytes("col_family"), Bytes.toBytes("member_id"), Bytes.toBytes(txn.getMember_id()));
			p.addColumn(Bytes.toBytes("col_family"), Bytes.toBytes("amount"), Bytes.toBytes(txn.getAmount()));
			p.addColumn(Bytes.toBytes("col_family"), Bytes.toBytes("postcode"), Bytes.toBytes(txn.getPostcode()));
			p.addColumn(Bytes.toBytes("col_family"), Bytes.toBytes("pos_id"), Bytes.toBytes(txn.getPos_id()));
			p.addColumn(Bytes.toBytes("col_family"), Bytes.toBytes("transaction_dt"),
					Bytes.toBytes(txn.getTransaction_dt()));
			p.addColumn(Bytes.toBytes("col_family"), Bytes.toBytes("status"), Bytes.toBytes(txn.getStatus()));

			System.out.println("Saving Card Transaction - " + txn.toString());
			cardTransactionsTable.put(p);

		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	// Method to create the connection for the Hbase lookup table
	@SuppressWarnings("deprecation")
	public static void getHbaseLookupTableConfig() {

		// Instantiating HTable class
		@SuppressWarnings("unused")
		Configuration config = HBaseConfiguration.create();
		try {
			Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();
			lookupTable = new HTable(hBaseAdmin1.getConfiguration(), "card_lookup_hbase");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	// Method to create the connection for the card transactions table
	@SuppressWarnings("deprecation")
	public static void getHbaseCardTransactionsTableConfig() {

		// Instantiating HTable class
		@SuppressWarnings("unused")
		Configuration config = HBaseConfiguration.create();
		try {
			Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();
			cardTransactionsTable = new HTable(hBaseAdmin1.getConfiguration(), "card_transactions_hbase");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
