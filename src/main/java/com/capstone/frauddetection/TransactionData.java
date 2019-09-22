package com.capstone.frauddetection;

public class TransactionData {
	
	private String memberId;
	private String memberScore;
	
	public String getMemberId() {
		return memberId;
	}
	public void setMemberId(String memberId) {
		this.memberId = memberId;
	}
	public String getMemberScore() {
		return memberScore;
	}
	public void setMemberScore(String memberScore) {
		this.memberScore = memberScore;
	}
	
	public TransactionData(String memberId){
		this.memberId=memberId;
	}
	
}
