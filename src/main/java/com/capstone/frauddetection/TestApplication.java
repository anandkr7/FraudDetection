package com.capstone.frauddetection;

public class TestApplication {

	public static void main(String[] args) {
		
		KafkaSparkStreaming kafkaStreaming = new KafkaSparkStreaming();
		//INSERT INTO TABLE card_transactions_hive(card_id,member_id,amount,postcode,pos_id,transaction_dt,status) 
		//VALUES('348702330256514','000037495066290','330148','33946','614677375609919','21-09-2019 00:00:00','GENUINE') ;
		//select * from card_transactions_hive where transaction_dt = "21-09-2019 00:00:00";
		
		KafkaTransaction txn = new KafkaTransaction();
		txn.setCard_id("348702330256000");
		txn.setMember_id("000037495066290");
		txn.setAmount("9084849");
		txn.setPostcode("33946");
		txn.setTransaction_dt("21-09-2019 15:41:00");
		txn.setPos_id("614677375609919");
		txn.setStatus("GENUINE");
		KafkaSparkStreaming.saveCardTransactionsData(txn);
		
		
		
	}
	
}
