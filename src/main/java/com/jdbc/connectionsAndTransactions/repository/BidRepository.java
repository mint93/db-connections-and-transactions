package com.jdbc.connectionsAndTransactions.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.springframework.stereotype.Repository;

import com.jdbc.connectionsAndTransactions.model.Bid;

@Repository
public class BidRepository {

	public int save(Connection connection, Bid bid) throws SQLException {
		String sql = "insert into bids (user,"
				+ " time, amount, currency) values (?, ?, ?"
				+ ", ?)";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, bid.getUser());
		preparedStatement.setTimestamp(2, Timestamp.valueOf(bid.getTime()));
		preparedStatement.setInt(3, bid.getAmount());
		preparedStatement.setString(4, bid.getCurrency());
		int rowsAffected = preparedStatement.executeUpdate();
		return rowsAffected;
	}
	
	public void createTable(Connection conn) throws SQLException {
		conn.createStatement().execute("create table bids "
				+ "(id identity, user VARCHAR, time TIMESTAMP ,"
				+ " amount NUMBER, currency VARCHAR)");
	}
	
	public void dropTable(Connection conn) throws SQLException {
		conn.createStatement().executeUpdate("drop table bids");
	}
	
}
