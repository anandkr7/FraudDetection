package com.capstone.frauddetection;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
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

	public static Integer getScore(TransactionData transactionData) throws IOException {

		Admin hBaseAdmin1 = HbaseConnection.getHbaseAdmin();
		HTable table = null;

		try {

			Get g = new Get(Bytes.toBytes(transactionData.getMemberId()));
			table = new HTable(hBaseAdmin1.getConfiguration(), "member_lookup_table");
			Result result = table.get(g);
			byte[] value = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("member_score"));
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
		return null;
	}

}
