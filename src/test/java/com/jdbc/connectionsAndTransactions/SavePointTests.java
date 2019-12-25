package com.jdbc.connectionsAndTransactions;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.TestPropertySource;

import com.jdbc.connectionsAndTransactions.jdbc.JdbcConnectionManager;
import com.jdbc.connectionsAndTransactions.repository.ItemRepository;
import com.jdbc.connectionsAndTransactions.util.Util;

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
public class SavePointTests {

	private JdbcConnectionManager jdbcConnectionManager = new JdbcConnectionManager(Util.connectionUrl);
	
	private ItemRepository itemRepository = new ItemRepository();
	
	@BeforeEach
	public void createTables() {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::createTable);
	}
	
	@AfterEach
	public void tearDown() {
		jdbcConnectionManager.executeOnNewConnection(itemRepository::dropTable);
	}
	
	@Test
	public void rollbackToSavepoint_ShouldRevertStatementsBetween() throws SQLException {
		try (Connection connection = jdbcConnectionManager.createConnection()) {
			connection.setAutoCommit(false);
			Savepoint savepoint = connection.setSavepoint("mySavePoint");
			itemRepository.save(connection, "CTU Field Agent Report");
			itemRepository.save(connection, "Chloeys Items");
			connection.rollback(savepoint);
			itemRepository.save(connection, "Nuclear Bomb");
			connection.commit();
		}
		List<String> names = itemRepository.findNames(jdbcConnectionManager.createConnection());
		assertThat(names.size()).isEqualTo(1);
		assertThat(names.get(0)).isEqualTo("Nuclear Bomb");
	}
	
}
