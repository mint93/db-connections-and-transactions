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
import com.jdbc.connectionsAndTransactions.model.Item;
import com.jdbc.connectionsAndTransactions.repository.BidRepository;
import com.jdbc.connectionsAndTransactions.repository.ItemRepository;
import com.jdbc.connectionsAndTransactions.util.JdbcUtil;

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
class AutocommitTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(JdbcUtil.username, JdbcUtil.password, JdbcUtil.connectionUrl);
	
	private BidRepository bidRepository = new BidRepository();
	private ItemRepository itemRepository = new ItemRepository();
	
	@BeforeEach
	public void createTables() {
		jdbcConnectionManager.executeOnNewConnection(this::createTables);
	}
	
	@AfterEach
	public void tearDown() {
		jdbcConnectionManager.executeOnNewConnection(this::dropTables);
	}
	
	@Test
	public void autocommitEnabled_ShouldRunEachStatementInSeperateTransaction() {
		jdbcConnectionManager.executeOnNewConnection(connection -> {
			itemRepository.save(connection, new Item("Windows 10 Premium Edition"));
			bidRepository.save(connection, new Bid("Hans", 1, "EUR"));
			bidRepository.save(connection, new Bid("Franz", 2, "EUR"));
			// Because of autocommit=true: even though error occurred during insert,
			// all previous data is persisted in separated transactions
			JdbcUtil.insertInvalidBid(connection);
		});
		assertThat(getBidsCount()).isEqualTo(2);
	}
	
	@Test
	public void autocommitDisabled_ShouldRunAllStatementsInOneTransaction() {
		jdbcConnectionManager.executeOnNewConnection(connection -> {
			// this is the ONLY way you start a transaction with plain JDBC
			connection.setAutoCommit(false);
			// the three statements are sent to the database, but not
			// yet commmited, not visible to other users/database connections
			// (assuming isolation level READ_COMMITTED)
			itemRepository.save(connection, new Item("Windows 10 Premium Edition"));
			bidRepository.save(connection, new Bid("Hans", 1, "EUR"));
			bidRepository.save(connection, new Bid("Franz", 2, "EUR"));
			// Because of autocommit=false: all operations are in the same
			// transaction, so when error occurred during insert, transaction
			// was not commited and all previous data was not persisted
			JdbcUtil.insertInvalidBid(connection);
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
