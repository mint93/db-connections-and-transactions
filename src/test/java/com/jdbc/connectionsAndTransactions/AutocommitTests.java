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
class AutocommitTests {

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
	public void tearDown() {
		jdbcConnectionManager.executeOnNewConnection(this::dropTables);
	}
	
	@Test
	public void autocommitEnabled_ShouldRunEachStatementInSeperateTransaction() {
		try (Connection connection = jdbcConnectionManager.createConnection()) {
			itemRepository.save(connection, "Windows 10 Premium Edition");
			bidRepository.save(connection, bid1);
			bidRepository.save(connection, bid2);
			Util.insertInvalidBid(connection);
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Because of autocommit=true: even though error occurred during insert, all previous data is persisted in seperated transactions");
		}
		assertThat(getBidsCount()).isEqualTo(2);
	}
	
	@Test
	public void autocommitDisabled_ShouldRunAllStatementsInOneTransaction() {
		try (Connection connection = jdbcConnectionManager.createConnection()) {
			// this is the ONLY way you start a transaction with plain JDBC
			connection.setAutoCommit(false);
			// the three statements are sent to the database, but not
			// yet commmited, not visible to other users/database connections
			itemRepository.save(connection, "Windows 10 Premium Edition");
			bidRepository.save(connection, bid1);
			bidRepository.save(connection, bid2);
			Util.insertInvalidBid(connection);
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Because of autocommit=false: all operations are in the same transaction, so when error occurred during insert, transaction was not commited and all previous data was not persisted");
		}
		assertThat(getBidsCount()).isZero();
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
