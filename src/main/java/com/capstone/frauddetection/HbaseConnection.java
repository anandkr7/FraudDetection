package com.capstone.frauddetection;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseConnection implements Serializable {

	private static final long serialVersionUID = 1L;
	private static Admin hbaseAdmin = null;
	
	public static Admin getHbaseAdmin() throws IOException {

		try {

			if (hbaseAdmin == null) {
				org.apache.hadoop.conf.Configuration conf = (org.apache.hadoop.conf.Configuration) HBaseConfiguration
						.create();
				conf.setInt("timeout", 1200);
				conf.set("hbase.master", "ec2-1-2-3-4.compute-1.amazonaws.com:60000");
				conf.set("hbase.zookeeper.quorum", "ec2-1-2-3-4.compute-1.amazonaws.com");
				conf.set("hbase.zookeeper.property.clientPort", "2181");
				conf.set("zookeeper.znode.parent", "/hbase");
				Connection con = ConnectionFactory.createConnection(conf);
				hbaseAdmin = con.getAdmin();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return hbaseAdmin;
	}

}
