package com.capstone.frauddetection;

import java.io.IOException;

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
	
	public static void main(String[] args) {
		try {
			int memberScore = HbaseDAO.getScore(new TransactionData("340028465709212"));
			System.out.println("memberScore -- " + memberScore);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
