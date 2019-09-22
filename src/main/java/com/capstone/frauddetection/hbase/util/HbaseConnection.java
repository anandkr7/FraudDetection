package com.capstone.frauddetection.hbase.util;

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
				conf.set("hbase.master", "quickstart.cloudera:60000");
				conf.set("hbase.zookeeper.quorum", "quickstart.cloudera");
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
