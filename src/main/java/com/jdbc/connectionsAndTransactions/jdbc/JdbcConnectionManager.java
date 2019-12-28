package com.jdbc.connectionsAndTransactions.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.springframework.stereotype.Component;

@Component
public class JdbcConnectionManager {

	private final String username;
	private final String password;
	private final String connectionUrl;

	public JdbcConnectionManager(String username, String password, String connection) {
		this.username = username;
		this.password = password;
		this.connectionUrl = connection;
	}
	
	public Connection createConnection() {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(connectionUrl, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
		
	}
	
	public void executeOnNewConnection(ThrowingConsumer<Connection, SQLException> onGetConnection) {
		executeOnNewConnection(onGetConnection, null);
	}
	public void executeOnNewConnection(ThrowingConsumer<Connection, SQLException> onGetConnection, ThrowingConsumer<Connection, SQLException> onException) {
		Connection connection = null;
		try {
			connection = createConnection();
			onGetConnection.accept(connection);
		} catch (SQLException e) {
			if (onException != null) {				
				try {
					onException.accept(connection);
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
			e.printStackTrace();
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@FunctionalInterface
	public interface ThrowingConsumer<T, E extends Throwable> {
	    void accept(T t) throws E;
	}
	
}
