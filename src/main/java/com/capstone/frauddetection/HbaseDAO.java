package com.capstone.frauddetection;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * HBase DAO class that provides different operational handlers.
 * 
 */

public class HbaseDAO {

	/**
	 *
	 * 
	 * 
	 * @param transactionData
	 * 
	 * @return get member's score from look up HBase table.
	 * 
	 * @throws IOException
	 * 
	 */

	public static int getScore(TransactionData transactionData) throws IOException {

		Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();
		HTable table = null;

		try {

			Get g = new Get(Bytes.toBytes(transactionData.getMemberId()));
			table = new HTable(hBaseAdmin1.getConfiguration(), "card_lookup_hbase");
			Result result = table.get(g);
			byte[] value = result.getValue(Bytes.toBytes("col_family"), Bytes.toBytes("score"));
			if (value != null) {
				return Integer.parseInt(Bytes.toString(value));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (table != null)
					table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return 0;
	}
	
	public static Double getUCLForTransaction(TransactionData transactionData) throws IOException {

		Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();
		HTable table = null;

		try {

			Get g = new Get(Bytes.toBytes(transactionData.getMemberId()));
			table = new HTable(hBaseAdmin1.getConfiguration(), "card_lookup_hbase");
			Result result = table.get(g);
			byte[] value = result.getValue(Bytes.toBytes("col_family"), Bytes.toBytes("UCL"));
			if (value != null) {
				return Double.parseDouble(Bytes.toString(value));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (table != null)
					table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return 0d;
	}
	
	public static String getPostCodeForTransaction(TransactionData transactionData) throws IOException {

		Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();
		HTable table = null;

		try {

			Get g = new Get(Bytes.toBytes(transactionData.getMemberId()));
			table = new HTable(hBaseAdmin1.getConfiguration(), "card_lookup_hbase");
			Result result = table.get(g);
			byte[] value = result.getValue(Bytes.toBytes("col_family"), Bytes.toBytes("postcode"));
			if (value != null) {
				return Bytes.toString(value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (table != null)
					table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static String getTxnTimeForTransaction(TransactionData transactionData) throws IOException {

		Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();
		HTable table = null;

		try {

			Get g = new Get(Bytes.toBytes(transactionData.getMemberId()));
			table = new HTable(hBaseAdmin1.getConfiguration(), "card_lookup_hbase");
			Result result = table.get(g);
			byte[] value = result.getValue(Bytes.toBytes("col_family"), Bytes.toBytes("transaction_dt"));
			if (value != null) {
				return Bytes.toString(value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (table != null)
					table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	public static String saveHbaseLookupData(KafkaTransaction txn, String currentPostcode, String currentTxnDt) {

		// Instantiating HTable class
		HTable hTable;
		try {
			
			// Instantiating Configuration class
			Configuration config = HBaseConfiguration.create();
			Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();
			hTable = new HTable(hBaseAdmin1.getConfiguration(), "card_lookup_hbase");
		
			// Instantiating Put class
			// accepts a row name
			Put p = new Put(Bytes.toBytes(txn.getCard_id()));

			// Updating a cell value
			p.add(Bytes.toBytes("col_family"), Bytes.toBytes("postcode"),
					Bytes.toBytes(txn.getPostcode()));
			p.add(Bytes.toBytes("col_family"), Bytes.toBytes("transaction_dt"),
					Bytes.toBytes(txn.getTransaction_dt()));

			// Saving the put Instance to the HTable.
			hTable.put(p);
			System.out.println("Data Updated -- Current PostCode - " + currentPostcode 
					+ " --  Updated Postcode - " + txn.getPostcode() 
					+ " %%%%% Current Date - " + currentTxnDt 
					+ " -- Updated Date - " + txn.getTransaction_dt());

			// closing HTable
			hTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

}
