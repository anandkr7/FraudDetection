package com.capstone.frauddetection;

import java.io.IOException;
import java.math.BigDecimal;

import com.capstone.frauddetection.hbase.util.HbaseDAO;
import com.capstone.frauddetection.hive.util.HiveDAO;
import com.capstone.fraudetection.util.DateUtility;
import com.google.gson.Gson;

public class KafkaSparkService {

	public static String validateCardTransaction(String data) {

		String postCode = "";
		String transactionDt = "";
		boolean genuineFlag = true;
		Gson gson = new Gson();

		try {
			if (data.length() > 10) {

				KafkaTransaction txn = gson.fromJson(data, KafkaTransaction.class);

				int memberScore = HbaseDAO.getScore(new TransactionData(txn.getCard_id()));
				Double uclValue = HbaseDAO.getUCLForTransaction(new TransactionData(txn.getCard_id()));
				postCode = HbaseDAO.getPostCodeForTransaction(new TransactionData(txn.getCard_id()));
				transactionDt = HbaseDAO.getTxnTimeForTransaction(new TransactionData(txn.getCard_id()));

				if (memberScore > 0) {

					if (Double.valueOf(txn.getAmount()) < new BigDecimal(uclValue).doubleValue()) {
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

					Double distance = distanceUtility.getDistanceViaZipCode(txn.getPostcode(), postCode);
					Double distancePerSecond = distance / ((DateUtility.getMilliseconds(txn.getTransaction_dt())
							- DateUtility.getMilliseconds(transactionDt)) / 1000);

					System.out.println("Card Id -- " + txn.getCard_id() + " Distance -- "
							+ new BigDecimal(distance).doubleValue() + " === Distance Per Second -- "
							+ new BigDecimal(distancePerSecond).toPlainString());

					if (distancePerSecond < 0.25) {
						genuineFlag = true;
					} else {
						genuineFlag = false;
					}

					if (genuineFlag) {
						txn.setStatus("GENUINE");
					} else {
						txn.setStatus("FRAUD");
					}

					HiveDAO.saveCardTransactionsData(txn);
					if (genuineFlag) {
						HbaseDAO.saveHbaseLookupData(txn, postCode, transactionDt);
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return "";
	}

}
