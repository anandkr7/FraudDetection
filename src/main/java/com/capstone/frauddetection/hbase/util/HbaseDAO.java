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

public class HbaseDAO {

	private static HTable lookupTable = null;
	private static HTable cardTransactionsTable = null;

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
		} finally {
		}
		return 0;
	}

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
		} finally {
		}
		return 0d;
	}

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
		} finally {

		}
		return null;
	}

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
		} finally {

		}
		return null;
	}

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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

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

			// Saving the put Instance to the HTable.
			System.out.println("Saving Card Transaction - " + txn.toString());
			lookupTable.put(p);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

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
