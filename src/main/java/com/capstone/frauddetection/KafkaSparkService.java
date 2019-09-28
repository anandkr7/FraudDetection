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

	// Method to fetch the previous card member details and calculate the
	// genuineness of the transaction
	public static void validateCardTransaction(String data, String zipCodeCVS) {

		String postCode = "";
		String transactionDt = "";
		boolean genuineFlag = true;
		Gson gson = new Gson();

		System.out.println("\nKafka Txn Data Received - " + data);
		KafkaTransaction txn = null;

		try {
			if (data.length() > 10) {

				// Fetch the member score, uclValue, postCode, transaction date from the lookup
				// table which will be updated constantly with the fresh data
				txn = gson.fromJson(data, KafkaTransaction.class);
				Integer memberScore = HbaseDAO.getScore(new TransactionData(txn.getCard_id()));
				Double uclValue = HbaseDAO.getUCLForTransaction(new TransactionData(txn.getCard_id()));
				postCode = HbaseDAO.getPostCodeForTransaction(new TransactionData(txn.getCard_id()));
				transactionDt = HbaseDAO.getTxnTimeForTransaction(new TransactionData(txn.getCard_id()));

				if (memberScore > 0) {

					// Using the card member details stored in lookup table, calculate the
					// genuineness of the transaction
					genuineFlag = validatTransaction(txn, memberScore, uclValue, postCode, transactionDt, zipCodeCVS);

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
	}

	private static boolean validatTransaction(KafkaTransaction txn, Integer memberScore, Double uclValue,
			String postCode, String transactionDt, String zipCodeCVS) throws NumberFormatException, IOException {

		boolean genuineFlag = true;

		// Check if the current transaction amount is more than the UCL value
		if (Double.valueOf(txn.getAmount()) > new BigDecimal(uclValue).doubleValue()) {
			// If the current transaction amount is greated than the UCL value the
			// transaction is fraudulent
			genuineFlag = false;
		}

		// Check if the current member score is less than 200.
		if (memberScore < 200) {
			// If the current card member score is less than 200, the transaction is
			// fraudulent
			genuineFlag = false;
		}

		// Check if the distance traveled per second between current transaction post
		// code and the previous transaction post code is less than 0.25 km/sec
		DistanceUtility distanceUtility = new DistanceUtility(zipCodeCVS);
		Double distance = distanceUtility.getDistanceViaZipCode(txn.getPostcode(), postCode);

		if (distance > 0) {
			double timeDiffInMillis = DateUtility.getMilliseconds(txn.getTransaction_dt())
					- DateUtility.getMilliseconds(transactionDt);

			if (timeDiffInMillis > 1000) {
				double timeDiffInSec = timeDiffInMillis / 1000;
				Double distancePerSecond = distance / timeDiffInSec;
				System.out.println(
						"Card Id -- " + txn.getCard_id() + " Distance -- " + new BigDecimal(distance).doubleValue()
								+ " === Distance Per Second -- " + new BigDecimal(distancePerSecond).toPlainString());

				// If the distance traveled is greater than the 0.25 km/sec betweeen two
				// transactions, than the transaction is fraudulent
				if (distancePerSecond > 0.25) {
					genuineFlag = false;
				}
			}
		}

		return genuineFlag;
	}

}
