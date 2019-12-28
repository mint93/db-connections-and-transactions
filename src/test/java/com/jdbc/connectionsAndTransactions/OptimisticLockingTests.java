package com.jdbc.connectionsAndTransactions;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.TestPropertySource;

import com.jdbc.connectionsAndTransactions.jdbc.JdbcConnectionManager;
import com.jdbc.connectionsAndTransactions.model.Item;
import com.jdbc.connectionsAndTransactions.repository.ItemRepository;
import com.jdbc.connectionsAndTransactions.repository.ItemVersionedRepository;
import com.jdbc.connectionsAndTransactions.repository.ItemVersionedRepository.OptimisticLockingException;
import com.jdbc.connectionsAndTransactions.util.JdbcUtil;

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
public class OptimisticLockingTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(JdbcUtil.username, JdbcUtil.password, JdbcUtil.connectionUrl);
	
	private ItemRepository itemRepository = new ItemVersionedRepository();
	
	private Item item = new Item("CTU Field Agent Report", 0);
		
	@BeforeEach
	public void createTables() {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::createTable);
	}
	
	@AfterEach
	public void tearDown() {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::dropTable);
	}
	
	@Test
	public void whenUpdatedRowsCount_ForOldVersionIsZero_ThenExceptionShouldBeThrown() throws SQLException {
		populateItemsTable();
		jdbcConnectionManager.executeOnNewConnection(connection1 -> {
			connection1.setAutoCommit(false);
			Item foundItem = itemRepository.findByName(connection1, item.getName()).get();
			jdbcConnectionManager.executeOnNewConnection(connection2 -> {
				connection2.setAutoCommit(false);
				Item updatedItem = itemRepository.save(connection2, new Item(item.getId(), item.getName(), item.getReleaseDate().plusDays(1), item.getVersion()));
				assertThat(updatedItem.getVersion()).isEqualTo(1);
				connection2.commit();
			});
			Assertions.assertThrows(OptimisticLockingException.class, () -> {				
				itemRepository.save(connection1, new Item(foundItem.getId(), foundItem.getName(), foundItem.getReleaseDate().plusDays(10), foundItem.getVersion()));
				connection1.rollback();
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
