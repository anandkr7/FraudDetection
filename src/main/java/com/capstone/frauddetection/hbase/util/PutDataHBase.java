package com.capstone.frauddetection.hbase.util;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.capstone.frauddetection.KafkaTransaction;

public class PutDataHBase {

	public static void putTransactionData(KafkaTransaction txnData, String status) throws IOException, ParseException {
		// Setting logging level to Warnning
		Logger.getRootLogger().setLevel(Level.WARN);

		SimpleDateFormat stream_format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		Date trDate = stream_format.parse(txnData.getTransaction_dt());
		long ts = trDate.getTime();

		// Set the configuration to connect hbase
		// If its local we are passing the internal IP
		// And setting the other needed parameters.
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("core-site.xml");
		conf.addResource("hbase-site.xml");
		conf.addResource("hdfs-site.xml");
		/*
		 * conf.setInt("timeout", 120000); conf.set("zookeeper.znode.parent", "/hbase");
		 * conf.set("hbase.zookeeper.property.clientPort", "2181");
		 * conf.set("hbase.zookeeper.quorum", "ip-172-31-86-194.ec2.internal");
		 * conf.set("hbase.master", "ip-172-31-86-194.ec2.internal:60000");
		 */

		System.out.println("Connecting to the HBase server...");
		Connection con = ConnectionFactory.createConnection(conf);
		System.out.println("Connected to Hbase");
		Admin admin = con.getAdmin();
		// get a Table object for the table

		// Name of the table
		String table = "cardTransactionDetails";

		// Check if table exists, Card_transactoins table

		if (admin.tableExists(TableName.valueOf(table))) {
			// Get a Table object
			Table htable = con.getTable(TableName.valueOf(table));

			try {

				String amount = txnData.getAmount().toString();
				String rowKey = txnData.getCard_id() + ":" + amount + ":" + txnData.getTransaction_dt();

				Put p = new Put(Bytes.toBytes(rowKey));

				p.addColumn(Bytes.toBytes("memberDetails"), Bytes.toBytes("card_id"),
						Bytes.toBytes(txnData.getCard_id()));
				p.addColumn(Bytes.toBytes("memberDetails"), Bytes.toBytes("member_id"),
						Bytes.toBytes(txnData.getMember_id()));
				p.addColumn(Bytes.toBytes("transactionDetails"), Bytes.toBytes("amount"),
						Bytes.toBytes(txnData.getAmount()));
				p.addColumn(Bytes.toBytes("locationDetails"), Bytes.toBytes("postcode"),
						Bytes.toBytes(txnData.getPostcode()));
				p.addColumn(Bytes.toBytes("locationDetails"), Bytes.toBytes("pos_id"),
						Bytes.toBytes(txnData.getPos_id()));
				p.addColumn(Bytes.toBytes("transactionDetails"), Bytes.toBytes("transaction_dt"),
						Bytes.toBytes(Long.toString(ts)));
				p.addColumn(Bytes.toBytes("transactionDetails"), Bytes.toBytes("status"), Bytes.toBytes(status));

				htable.put(p);

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if (htable != null)
						htable.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} else {
			System.out.println("cardTransactionDetails table for put data does not exist");
		}

	}

}
