package com.jdbc.connectionsAndTransactions;

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
import com.jdbc.connectionsAndTransactions.repository.BidRepository;
import com.jdbc.connectionsAndTransactions.repository.ItemRepository;
import com.jdbc.connectionsAndTransactions.util.Util;

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
class RollbackTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(Util.connectionUrl);
	
	private BidRepository bidRepository = new BidRepository();
	private ItemRepository itemRepository = new ItemRepository();
	
	private final Bid bid1 = Bid.builder()
			.user("Hans")
			.amount(1)
			.currency("EUR")
			.build();
	private final Bid bid2 = Bid.builder()
			.user("Franz")
			.amount(2)
			.currency("EUR")
			.build();
	
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
		try (Connection connection = jdbcConnectionManager.createConnection()) {
			connection.setAutoCommit(false);
			itemRepository.save(connection, "Windows 10 Premium Edition");
			bidRepository.save(connection, bid1);
			bidRepository.save(connection, bid2);
			// rollback() undoes any changes in the current transaction.
			// It must be triggered before commit(), because when you call commit(),
			// you complete/close the current transaction.
			connection.rollback();
			connection.commit();
		}
		int bidsCount = getBidsCount();
		assertThat(bidsCount).isZero();
	}
	
	private int getBidsCount() {
		return Util.getRowsCountFromTable(jdbcConnectionManager.createConnection(), "bids");
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
