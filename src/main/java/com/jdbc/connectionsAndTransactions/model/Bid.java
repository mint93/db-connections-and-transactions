package com.jdbc.connectionsAndTransactions.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Bid {
	private int id;
	private String user;
	private int amount;
	private String currency;
}
