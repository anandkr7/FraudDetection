package com.capstone.frauddetection;

/**
 * Model DTO Class for holding the crad transaction details
 */
public class KafkaTransaction {

	private String card_id;
	private String member_id;
	private String postcode;
	private String amount;
	private String transaction_dt;
	private String pos_id;
	private String status;

	public String getPos_id() {
		return pos_id;
	}

	public void setPos_id(String pos_id) {
		this.pos_id = pos_id;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getCard_id() {
		return card_id;
	}

	public void setCard_id(String card_id) {
		this.card_id = card_id;
	}

	public String getMember_id() {
		return member_id;
	}

	public void setMember_id(String member_id) {
		this.member_id = member_id;
	}

	public String getPostcode() {
		return postcode;
	}

	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}

	public String getAmount() {
		return amount;
	}

	public void setAmount(String amount) {
		this.amount = amount;
	}

	@Override
	public String toString() {
		return "KafkaTransaction [card_id=" + card_id + ", member_id=" + member_id + ", postcode=" + postcode
				+ ", amount=" + amount + ", transaction_dt=" + transaction_dt + ", pos_id=" + pos_id + ", status="
				+ status + "]";
	}

	public String getTransaction_dt() {
		return transaction_dt;
	}

	public void setTransaction_dt(String transaction_dt) {
		this.transaction_dt = transaction_dt;
	}

}
