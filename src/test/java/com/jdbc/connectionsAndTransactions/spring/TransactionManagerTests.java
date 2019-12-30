package com.jdbc.connectionsAndTransactions.spring;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:h2-test-db.properties")
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
class TransactionManagerTests {
	// null for defaults transaction settings
	private static final TransactionDefinition TX_DEFAULTS = null;
	
	@Autowired
	private DataSource ds;
	
	@Autowired
	private PlatformTransactionManager txManager;
	
	@Test
	public void testConnection() {
		try (Connection connection = ds.getConnection()) {
			assertTrue(connection.isValid(1000));
		} catch (Exception e) {
			e.printStackTrace();
		}
		// open and commit transaction
		TransactionStatus transaction =	txManager.getTransaction(TX_DEFAULTS);
		txManager.commit(transaction);
	}
	
	@Configuration
    static class ContextConfiguration {
		@Bean
		public DataSource dataSource() {
			JdbcDataSource ds = new JdbcDataSource();
			ds.setURL("jdbc:h2:mem:db;DB_CLOSE_DELAY=-1");
			ds.setUser("sa");
			return ds;
		}
		
		@Bean
		public PlatformTransactionManager txManager() {
			return new DataSourceTransactionManager(dataSource());
		}
    }

}