package com.capstone.frauddetection;

import java.io.IOException;
import java.math.BigDecimal;

import com.capstone.frauddetection.hbase.util.HbaseDAO;
import com.capstone.fraudetection.util.DateUtility;
import com.google.gson.Gson;

/**
 * Class that reads the transactions from the Kafka Streams and performs the
 * processing of the 3 Rules and .
 *
 */
public class KafkaSparkService {

	public static String validateCardTransaction(String data, String zipCodeCVS) {

		String postCode = "";
		String transactionDt = "";
		boolean genuineFlag = true;
		Gson gson = new Gson();

		System.out.println("\nKafka Txn Data Received - " + data);

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

					DistanceUtility distanceUtility = new DistanceUtility(zipCodeCVS);
					Double distance = distanceUtility.getDistanceViaZipCode(txn.getPostcode(), postCode);

					if (distance > 0) {

						double timeDiffInMillis = DateUtility.getMilliseconds(txn.getTransaction_dt())
								- DateUtility.getMilliseconds(transactionDt);

						if (timeDiffInMillis > 1000) {
							double timeDiffInSec = timeDiffInMillis / 1000;
							Double distancePerSecond = timeDiffInSec / timeDiffInMillis;
							System.out.println("Card Id -- " + txn.getCard_id() + " Distance -- "
									+ new BigDecimal(distance).doubleValue() + " === Distance Per Second -- "
									+ new BigDecimal(distancePerSecond).toPlainString());

							if (distancePerSecond < 0.25) {
								genuineFlag = true;
							} else {
								genuineFlag = false;
							}
						} else {
							genuineFlag = true;
						}
					} else {
						genuineFlag = true;
					}

					if (genuineFlag) {
						txn.setStatus("GENUINE");
					} else {
						txn.setStatus("FRAUD");
					}

					HbaseDAO.saveCardTransactionsHbase(txn);
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
