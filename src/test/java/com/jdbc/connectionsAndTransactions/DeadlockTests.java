package com.jdbc.connectionsAndTransactions;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.jdbc.JdbcSQLTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.TestPropertySource;

import com.jdbc.connectionsAndTransactions.jdbc.JdbcConnectionManager;
import com.jdbc.connectionsAndTransactions.repository.ItemRepository;
import com.jdbc.connectionsAndTransactions.util.Util;

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
public class DeadlockTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(Util.connectionUrl);
	
	private ItemRepository itemRepository = new ItemRepository();
	
	@AfterEach
	public void tearDown() {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::dropTable);
	}
	
	@Test
	public void insertTheSameNameInParallelTransactions_WithoutUniqueConstraint_ShouldBeValid() throws SQLException {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::createTable);
		try (Connection connection1 = jdbcConnectionManager.createConnection()) {
			connection1.setAutoCommit(false);
			itemRepository.save(connection1, "CTU Field Agent Report");
			try (Connection connection2 = jdbcConnectionManager.createConnection()) {
				connection2.setAutoCommit(false);
				itemRepository.save(connection2, "CTU Field Agent Report");
				connection2.commit();
			}
			connection1.commit();
		}
		int itemsCount = Util.getRowsCountFromTable(jdbcConnectionManager.createConnection(), "items");
		assertThat(itemsCount).isEqualTo(2);
	}
	
	@Test
	public void insertTheSameNameInParallelTransactions_WithUniqueConstraint_ShouldThrowException() throws SQLException {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::createTableWithUniqueName);
		try (Connection connection1 = jdbcConnectionManager.createConnection()) {
			connection1.setAutoCommit(false);
			itemRepository.save(connection1, "CTU Field Agent Report");
			try (Connection connection2 = jdbcConnectionManager.createConnection()) {
				connection2.setAutoCommit(false);
				// timeout trying to lock row with unique name
				// concurrent update in table 'ITEMS': another transaction
				// has updated or deleted the same row
				Assertions.assertThrows(JdbcSQLTimeoutException.class, () -> {				    
					itemRepository.save(connection2, "CTU Field Agent Report");
				});
				connection2.commit();
			}
			connection1.commit();
		}
	}
	
	@Test
	public void updateData_BeforeInsertIsCommited_ShouldNotThrowException() throws SQLException {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::createTable);
		try (Connection connection1 = jdbcConnectionManager.createConnection()) {
			connection1.setAutoCommit(false);
			itemRepository.save(connection1, "CTU Field Agent Report");
			try (Connection connection2 = jdbcConnectionManager.createConnection()) {
				connection2.setAutoCommit(false);
				connection2.createStatement().execute(
						"update items set name = 'destroyed' "
						+ "where name = 'CTU Field Agent Report'");
				connection2.commit();
			}
			connection1.commit();
			String name = itemRepository.findNames(jdbcConnectionManager.createConnection()).get(0);
			assertThat(name).isEqualTo("CTU Field Agent Report");
		}
	}
	
	@Test
	public void alterLockedTableInParallelTransactions_ShouldThrowException() throws SQLException {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::createTable);
		try (Connection connection1 = jdbcConnectionManager.createConnection()) {
			connection1.setAutoCommit(false);
			itemRepository.save(connection1, "CTU Field Agent Report");
			try (Connection connection2 = jdbcConnectionManager.createConnection()) {
				connection2.setAutoCommit(false);
				// Table is locked, only allowing concurrent reads,
				// but no more writes. All users will experience write blocks.
				Assertions.assertThrows(JdbcSQLTimeoutException.class, () -> {
					connection2.createStatement().execute(
							"alter table items add column (release_date date null)");
				});
				connection2.commit();
			}
			connection1.commit();
		}
	}
}
