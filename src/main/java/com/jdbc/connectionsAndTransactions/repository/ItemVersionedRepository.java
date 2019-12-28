package com.jdbc.connectionsAndTransactions.repository;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import org.springframework.stereotype.Repository;

import com.jdbc.connectionsAndTransactions.model.Item;

@Repository
public class ItemVersionedRepository extends ItemRepository {

	@Override
	protected Item insert(Connection connection, Item item) throws SQLException {
		String sql = "insert into items (name, release_date, version) values (?, ?, ?)";
		PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		preparedStatement.setString(1, item.getName());
		preparedStatement.setDate(2, Date.valueOf(item.getReleaseDate()));
		preparedStatement.setInt(3, 0);
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

	@Override
	protected Item update(Connection connection, Item newItem, Item oldItem) throws SQLException {
		String sql = "update items set name = ?, release_date = ?, version = ? where id = ? and version = ?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, newItem.getName());
		preparedStatement.setDate(2, Date.valueOf(newItem.getReleaseDate()));
		int actualVersion = newItem.getVersion();
		int newVersion = actualVersion+1;
		preparedStatement.setInt(3, newVersion);
		preparedStatement.setInt(4, oldItem.getId());
		preparedStatement.setInt(5, actualVersion);
		int affectedRows = preparedStatement.executeUpdate();
		if (affectedRows > 0) {
			return new Item(oldItem.getId(), newItem.getName(), newItem.getReleaseDate(), newVersion);
        } else {
        	throw new OptimisticLockingException();
        }
	}
	
	@Override
	public Optional<Item> findById(Connection connection, int id) throws SQLException {
		String sql = "select * from items where id = ?";
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.setInt(1, id);
		try (ResultSet resultSet = statement.executeQuery()) {
			if (resultSet.next()) {				
				return Optional.of(new Item(resultSet.getInt("id"), resultSet.getString("name"), resultSet.getDate("release_date").toLocalDate(), resultSet.getInt("version")));
			} else {
				return Optional.empty();
			}
		}
	}

	@Override
	public void createTable(Connection conn) throws SQLException {
		conn.createStatement().execute("create table items (id "
				+ "identity, release_date date, name VARCHAR,"
				+ " version NUMBER default 0)");
	}
	
	@Override
	public void createTableWithUniqueName(Connection conn) throws SQLException {
		conn.createStatement().execute("create table items (id "
				+ "identity, release_date date, name VARCHAR unique,"
				+ " version NUMBER default 0)");
	}
	
	public static class OptimisticLockingException extends RuntimeException {
		private static final long serialVersionUID = 1211444357929277049L;
	}
	
}
