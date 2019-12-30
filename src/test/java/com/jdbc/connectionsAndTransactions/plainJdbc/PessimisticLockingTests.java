package com.jdbc.connectionsAndTransactions.plainJdbc;

import java.sql.SQLException;

import org.h2.jdbc.JdbcSQLTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.TestPropertySource;

import com.jdbc.connectionsAndTransactions.jdbc.JdbcConnectionManager;
import com.jdbc.connectionsAndTransactions.model.Item;
import com.jdbc.connectionsAndTransactions.plainJdbc.util.JdbcUtil;
import com.jdbc.connectionsAndTransactions.repository.ItemRepository;

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
public class PessimisticLockingTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(JdbcUtil.username, JdbcUtil.password, JdbcUtil.connectionUrl);
	
	private ItemRepository itemRepository = new ItemRepository();
	
	private Item item = new Item("CTU Field Agent Report");
	
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
		populateItemsTable();
		jdbcConnectionManager.executeOnNewConnection(connection1 -> {
			connection1.setAutoCommit(false);
			// Acquiring an exclusive lock on the selected records (by "for update" statement)
			// Once the database records are locked, no UPDATE or DELETE statements can modify them
			// INSERT statement behaves differently based on database system
			connection1.createStatement().execute("select * from items where "
					+ "name = 'CTU Field Agent Report' for update");
			jdbcConnectionManager.executeOnNewConnection(connection2 -> {
				connection2.setAutoCommit(false);
				Assertions.assertThrows(JdbcSQLTimeoutException.class, () -> {
					itemRepository.save(connection2, new Item(item.getId(), item.getName(), item.getReleaseDate().plusDays(10), item.getVersion()));
				});
				connection2.commit();
			});
			connection1.commit();
		});
	}

	private void populateItemsTable() throws SQLException {
		jdbcConnectionManager.executeOnNewConnection(connection -> {
			connection.setAutoCommit(false);
			item = itemRepository.save(connection, item);
			connection.commit();
		});
	}
	
}
