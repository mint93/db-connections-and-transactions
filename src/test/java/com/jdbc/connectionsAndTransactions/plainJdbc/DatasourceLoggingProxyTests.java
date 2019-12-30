package com.jdbc.connectionsAndTransactions.plainJdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.TestPropertySource;

import com.jdbc.connectionsAndTransactions.model.Item;
import com.jdbc.connectionsAndTransactions.plainJdbc.util.JdbcUtil;
import com.jdbc.connectionsAndTransactions.repository.ItemRepository;
import com.p6spy.engine.spy.P6DataSource;

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
public class DatasourceLoggingProxyTests {

	private ItemRepository itemRepository = new ItemRepository();
	
	// SingleLineFormat (by default):
	// current time|execution time|category|connection id|statement SQL String|effective SQL string
	@Test
	public void testLogging() throws SQLException {
		DataSource datassource = getDataSource();
		try (Connection conn = datassource.getConnection()) {
			itemRepository.createTable(conn);
			itemRepository.save(conn, new Item("item name"));
			itemRepository.dropTable(conn);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private DataSource getDataSource() {
		JdbcDataSource datasource = new JdbcDataSource();
		datasource.setURL(JdbcUtil.connectionUrl);
		datasource.setUser(JdbcUtil.username);
		return new P6DataSource(datasource);
	}
	
}
