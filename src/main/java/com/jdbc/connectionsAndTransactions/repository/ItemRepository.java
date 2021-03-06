package com.jdbc.connectionsAndTransactions.repository;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Repository;

import com.jdbc.connectionsAndTransactions.model.Item;

@Repository
public class ItemRepository {

	public Item save(Connection connection, Item item) throws SQLException {
		Optional<Item> foundItem = item.getId() != null ? findById(connection, item.getId()) : Optional.empty();
		if (foundItem.isPresent()) {
			return update(connection, item, foundItem.get());
		} else {
			return insert(connection, item);
		}
	}

	protected Item insert(Connection connection, Item item) throws SQLException {
		String sql = "insert into items (name, release_date) values (?, ?)";
		PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		preparedStatement.setString(1, item.getName());
		preparedStatement.setDate(2, Date.valueOf(item.getReleaseDate()));
		int affectedRows = preparedStatement.executeUpdate();
		if (affectedRows > 0) {
            try (ResultSet rs = preparedStatement.getGeneratedKeys()) {
                if (rs.next()) {
                    return new Item(rs.getInt(1), item.getName(), item.getReleaseDate(), item.getVersion());
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
		return null;
	}

	protected Item update(Connection connection, Item newItem, Item oldItem) throws SQLException {
		String sql = "update items set name = ?, release_date = ? where id = ?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, newItem.getName());
		preparedStatement.setDate(2, Date.valueOf(newItem.getReleaseDate()));
		preparedStatement.setInt(3, oldItem.getId());
		preparedStatement.executeUpdate();
		return new Item(oldItem.getId(), newItem.getName(), newItem.getReleaseDate(), newItem.getVersion());
	}
	
	public Optional<Item> findById(Connection connection, int id) throws SQLException {
		String sql = "select * from items where id = ?";
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.setInt(1, id);
		try (ResultSet resultSet = statement.executeQuery()) {
			if (resultSet.next()) {				
				return Optional.of(new Item(resultSet.getInt("id"), resultSet.getString("name"), resultSet.getDate("release_date").toLocalDate(), null));
			} else {
				return Optional.empty();
			}
		}
	}
	
	public Optional<Item> findByName(Connection connection, String name) throws SQLException {
		String sql = "select * from items where name = ?";
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.setString(1, name);
		try (ResultSet resultSet = statement.executeQuery()) {
			if (resultSet.next()) {				
				return Optional.of(new Item(resultSet.getInt("id"), resultSet.getString("name"), resultSet.getDate("release_date").toLocalDate(), resultSet.getInt("version")));
			} else {
				return Optional.empty();
			}
		}
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
