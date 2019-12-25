package com.jdbc.connectionsAndTransactions.repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Repository;

@Repository
public class ItemRepository {

	public int save(Connection connection, String name) throws SQLException {
		String sql = "insert into items (name) values (?)";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, name);
		int rowsAffected = preparedStatement.executeUpdate();
		return rowsAffected;
	}
	
	public void createTable(Connection conn) throws SQLException {
		conn.createStatement().execute("create table items (id "
				+ "identity, release_date date, name VARCHAR)");
	}
	
	public void createTableWithUniqueName(Connection conn) throws SQLException {
		conn.createStatement().execute("create table items (id "
				+ "identity, release_date date, name VARCHAR unique)");
	}
	
	public void dropTable(Connection conn) throws SQLException {
		conn.createStatement().executeUpdate("drop table items");
	}
	
	public List<String> findNames(Connection conn) throws SQLException {
		try (ResultSet resultSet = conn.createStatement().executeQuery("select * from items;")) {
			List<String> names = new ArrayList<>();
			while (resultSet.next()) {
				names.add(resultSet.getString("name"));
			}
			return names;
		}
	}
	
}
