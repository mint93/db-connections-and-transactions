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

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
class RollbackTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager("jdbc:h2:mem:db;DB_CLOSE_DELAY=-1");
	
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
			Util.insertItem(connection);
			Util.insertBid1(connection);
			Util.insertBid2(connection);
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
		Util.createItems(conn);
		Util.createBids(conn);
	}
	
	private void dropTables(Connection conn) throws SQLException {
		Util.dropItems(conn);
		Util.dropBids(conn);
	}
	
}
