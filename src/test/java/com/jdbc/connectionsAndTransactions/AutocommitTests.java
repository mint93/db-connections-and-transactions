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
class AutocommitTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager("jdbc:h2:mem:db;DB_CLOSE_DELAY=-1");
	
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
			Util.insertItem(connection);
			Util.insertBid1(connection);
			Util.insertBid2(connection);
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
			Util.insertItem(connection);
			Util.insertBid1(connection);
			Util.insertBid2(connection);
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
		Util.createItems(conn);
		Util.createBids(conn);
	}
	
	private void dropTables(Connection conn) throws SQLException {
		Util.dropItems(conn);
		Util.dropBids(conn);
	}
	
}
