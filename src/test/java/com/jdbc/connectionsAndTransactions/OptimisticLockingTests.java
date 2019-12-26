package com.jdbc.connectionsAndTransactions;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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
public class OptimisticLockingTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(Util.connectionUrl);
	
	private ItemRepository itemRepository = new ItemRepository();
	
	@BeforeEach
	public void createTables() {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::createTableWithVersion);
	}
	
	@AfterEach
	public void tearDown() {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::dropTable);
	}
	
	@Test
	public void whenUpdatedRowsCount_ForOldVersionIsZero_ThenExceptionShouldBeThrown() throws SQLException {
		populateItemsTable();
		try (Connection connection1 = jdbcConnectionManager.createConnection()) {
			connection1.setAutoCommit(false);
			String version = "";
			try (ResultSet resultSet = connection1.createStatement().executeQuery("select * from items where name = 'CTU Field Agent Report'")) {
				resultSet.next();
				version = resultSet.getString("version");
			}
			try (Connection connection2 = jdbcConnectionManager.createConnection()) {
				connection2.setAutoCommit(false);
				int updatedRows = connection2.createStatement()
						.executeUpdate(
								"update items set release_date = current_date() + 10,"
								+ " version = version + 1"
								+ " where name = 'CTU Field Agent Report'"
								+ " and version = 0");
				assertThat(updatedRows).isEqualTo(1);
				connection2.commit();
			}
			int updatedRows = connection1.createStatement()
					.executeUpdate(
							"update items set release_date = current_date(), "
							+ " version = version + 1 "
							+ "where name = 'CTU Field Agent Report'"
							+ " and version = " + version);
			Assertions.assertThrows(OptimisticLockingException.class, () -> {				
				if (updatedRows == 0) {
					connection1.rollback();
					throw new OptimisticLockingException();
				}
			});
			connection1.commit();
		}
		
	}

	private void populateItemsTable() throws SQLException {
		jdbcConnectionManager.executeOnNewConnection(connection -> {			
			connection.setAutoCommit(false);
			connection.createStatement().execute(
					"insert into items "
							+ "(name, release_date, version) values "
							+ "('CTU Field Agent "
							+ "Report', current_date() - 100, 0)");
			connection.commit();
		});
	}
	
	public static class OptimisticLockingException extends RuntimeException {
		private static final long serialVersionUID = 1211444357929277049L;
	}
	
}
