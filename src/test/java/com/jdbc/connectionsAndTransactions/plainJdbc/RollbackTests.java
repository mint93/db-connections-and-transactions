package com.jdbc.connectionsAndTransactions.plainJdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.TestPropertySource;

import com.jdbc.connectionsAndTransactions.jdbc.JdbcConnectionManager;
import com.jdbc.connectionsAndTransactions.model.Bid;
import com.jdbc.connectionsAndTransactions.model.Item;
import com.jdbc.connectionsAndTransactions.plainJdbc.util.JdbcUtil;
import com.jdbc.connectionsAndTransactions.repository.BidRepository;
import com.jdbc.connectionsAndTransactions.repository.ItemRepository;

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
class RollbackTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(JdbcUtil.username, JdbcUtil.password, JdbcUtil.connectionUrl);
	
	private BidRepository bidRepository = new BidRepository();
	private ItemRepository itemRepository = new ItemRepository();
	
	@BeforeEach
	public void createTables() {
		jdbcConnectionManager.executeOnNewConnection(this::createTables);
	}
	
	@AfterEach
	public void dropTables() {
		jdbcConnectionManager.executeOnNewConnection(this::dropTables);
	}
	
	@Test
	public void rollback_ShouldRevertTransaction_WhenAutoCommitDisabled() throws SQLException {
		jdbcConnectionManager.executeOnNewConnection(connection -> {
			connection.setAutoCommit(false);
			itemRepository.save(connection, new Item("Windows 10 Premium Edition"));
			bidRepository.save(connection, new Bid("Hans", 1, "EUR"));
			bidRepository.save(connection, new Bid("Franz", 2, "EUR"));
			// rollback() undoes any changes in the current transaction.
			// It must be triggered before commit(), because when you call commit(),
			// you complete/close the current transaction.
			connection.rollback();
			connection.commit();
		});
		assertThat(getBidsCount()).isZero();
	}
	
	private int getBidsCount() {
		return JdbcUtil.getRowsCountFromTable(jdbcConnectionManager.createConnection(), "bids");
	}
	
	private void createTables(Connection conn) throws SQLException {
		itemRepository.createTable(conn);
		bidRepository.createTable(conn);
	}
	
	private void dropTables(Connection conn) throws SQLException {
		itemRepository.dropTable(conn);
		bidRepository.dropTable(conn);
	}
	
}
