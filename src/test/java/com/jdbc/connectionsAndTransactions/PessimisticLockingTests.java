package com.jdbc.connectionsAndTransactions;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.jdbc.JdbcSQLTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.TestPropertySource;

import com.jdbc.connectionsAndTransactions.jdbc.JdbcConnectionManager;
import com.jdbc.connectionsAndTransactions.repository.ItemRepository;
import com.jdbc.connectionsAndTransactions.util.Util;

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
public class PessimisticLockingTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(Util.connectionUrl);
	
	private ItemRepository itemRepository = new ItemRepository();
	
	@BeforeEach
	public void createTables() {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::createTable);
	}
	
	@AfterEach
	public void tearDown() {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::dropTable);
	}
	
	@Test
	public void forUpdate_LocksRow_ThenParallelUpdate_ShouldThrowException() throws SQLException {
		try (Connection connection = jdbcConnectionManager.createConnection()) {
			connection.setAutoCommit(false);
			itemRepository.save(connection, "CTU Field Agent Report");
			connection.commit();
		}
		try (Connection connection1 = jdbcConnectionManager.createConnection()) {
			connection1.setAutoCommit(false);
			// Acquiring an exclusive lock on the selected records (by "for update" statement)
			// Once the database records are locked, no UPDATE or DELETE statements can modify them
			// INSERT statement behaves differently based on database system
			connection1.createStatement().execute("select * from items where "
					+ "name = 'CTU Field Agent Report' for update");
			try (Connection connection2 = jdbcConnectionManager.createConnection()) {
				connection2.setAutoCommit(false);
				Assertions.assertThrows(JdbcSQLTimeoutException.class, () -> {
					connection2.createStatement().executeUpdate(
							"update items set "
							+ "release_date = current_date() + 10"
							+ " where name = 'CTU Field Agent Report'");
				});
				connection2.commit();
			}
			connection1.commit();
		}
	}
	
}
