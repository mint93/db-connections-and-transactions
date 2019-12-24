package com.jdbc.connectionsAndTransactions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Util {
	
	private Util() {}
	
	public static int getRowsCountFromTable(Connection connection, String tableName) {
		try (ResultSet resultSet = connection.createStatement().executeQuery("select count(*) as amount from " + tableName + ";")) {
			int amount = 0;
			while(resultSet.next())
				amount = resultSet.getInt("amount");
			return amount;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public static List<Map<String, String>> getConnectionInformation(Connection conn) {
		List<Map<String, String>> connectionsInformations = new ArrayList<>();
		try (ResultSet resultSet = conn.createStatement().executeQuery("select * from information_schema.sessions;")) {
			while(resultSet.next()){
				Map<String, String> info = new HashMap<>();
				info.put("ID", String.valueOf(resultSet.getInt("ID")));
				info.put("USER_NAME", resultSet.getString("USER_NAME"));
				info.put("SERVER", resultSet.getString("SERVER"));
				info.put("CLIENT_ADDR", resultSet.getString("CLIENT_ADDR"));
				info.put("CLIENT_INFO", resultSet.getString("CLIENT_INFO"));
				info.put("SESSION_START", String.valueOf(resultSet.getTime("SESSION_START")));
				info.put("ISOLATION_LEVEL", resultSet.getString("ISOLATION_LEVEL"));
				info.put("STATEMENT", resultSet.getString("STATEMENT"));
				info.put("STATEMENT_START", String.valueOf(resultSet.getTime("STATEMENT_START")));
				info.put("CONTAINS_UNCOMMITTED", String.valueOf(resultSet.getBoolean("CONTAINS_UNCOMMITTED")));
				info.put("STATE", resultSet.getString("STATE"));
				info.put("BLOCKER_ID", String.valueOf(resultSet.getInt("BLOCKER_ID")));
				connectionsInformations.add(info);
			}
		} catch (SQLException e) {
			e.printStackTrace();	
		}
		return connectionsInformations;
	}
	
	
	
	
	
	public static void insertItem(Connection connection) throws SQLException {
		connection.createStatement().execute("insert into items (name)"
				+ " values ('Windows 10 Premium Edition')");
	}

	public static void insertBid1(Connection connection) throws SQLException {
		connection.createStatement().execute("insert into bids (user,"
				+ " time, amount, currency) values ('Hans', now(), 1"
				+ ", 'EUR')");
	}
	
	public static void insertBid2(Connection connection) throws SQLException {
		connection.createStatement().execute("insert into bids (user,"
				+ " time, amount, currency) values ('Franz',now() ,"
				+ " 2" + ", 'EUR')");
	}
	
	public static void insertInvalidBid(Connection connection) throws SQLException {
		connection.createStatement().execute("insert into bidz (user,"
				+ " time, amount, currency) values ('Frank',now() ,"
				+ " 3" + ", 'EUR')");
	}
	
	public static void createItems(Connection conn) throws SQLException {
		conn.createStatement().execute("create table items (id"
				+ " identity, name VARCHAR)");
	}
	
	public static void createItemsWithUniqueName(Connection conn) throws SQLException {
		conn.createStatement().execute("create table items (id " +
				"identity, name VARCHAR unique)");
	}
	
	public static void createBids(Connection conn) throws SQLException {
		conn.createStatement().execute("create table bids "
				+ "(id identity, user VARCHAR, time TIMESTAMP ,"
				+ " amount NUMBER, currency VARCHAR)");
	}
	
	public static void dropItems(Connection conn) throws SQLException {
		conn.createStatement().executeUpdate("drop table items");
	}
	
	public static void dropBids(Connection conn) throws SQLException {
		conn.createStatement().executeUpdate("drop table bids");
	}


}
