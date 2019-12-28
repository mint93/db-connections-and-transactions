package com.jdbc.connectionsAndTransactions.model;

import java.time.LocalDateTime;

import lombok.Data;

@Data
public class Bid {
	private final Integer id;
	private final String user;
	private final LocalDateTime time;
	private final int amount;
	private final String currency;
	
	public Bid(String user, int amount, String currency) {
		this(user, LocalDateTime.now(), amount, currency);
	}
	
	public Bid(String user, LocalDateTime time, int amount, String currency) {
		this.id = null;
		this.user = user;
		this.time = time;
		this.amount = amount;
		this.currency = currency;
	}
	
}
