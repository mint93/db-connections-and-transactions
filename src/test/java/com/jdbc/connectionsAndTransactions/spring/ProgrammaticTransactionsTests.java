package com.jdbc.connectionsAndTransactions.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;

import com.p6spy.engine.spy.P6DataSource;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:h2-test-db.properties")
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
class ProgrammaticTransactionsTests {
	
	@Autowired
	private BankTeller bankTeller;
	
	@Autowired
	private DataSource ds;
	
	@BeforeEach
	public void setUp() {
		try (Connection connection = ds.getConnection()) {
			createTables(connection);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	@AfterEach
	public void tearDown() {
		try (Connection connection = ds.getConnection()) {
			dropTables(connection);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void getAccountBalance_ForUser1_ShouldBeEqualTo100() throws SQLException {
		Long balance = bankTeller.getAccountBalance("User1");
		assertThat(balance).isEqualTo(100);
	}
	
	static class BankTeller {
		@Autowired
		private PlatformTransactionManager txManager;
		@Autowired
		private DataSource ds;
		
		public Long getAccountBalance(final String name) throws SQLException {
			Long balance = new TransactionTemplate(txManager).execute(transactionStatus -> {
				Long currentBalance = new JdbcTemplate(ds).queryForObject(
						"select balance from accounts " +
						"where name = ?", Long.class, name);
				return currentBalance;
			});
			return balance;
		}
	}
	
	private void createTables(Connection conn) throws SQLException {
		conn.createStatement().execute("create table if not exists "
				+ "accounts (name varchar primary key, balance bigint)");
		conn.createStatement().execute("create table if not exists "
				+ "account_activity (date_occurred date, description VARCHAR,"
				+ " name varchar, foreign key (name) references accounts(name))");
		conn.createStatement().execute("insert into accounts values"
				+ "('User1'," + 100 + ")");
	}
	
	private void dropTables(Connection conn) throws SQLException {
		conn.createStatement().executeUpdate("drop table account_activity");
		conn.createStatement().executeUpdate("drop table accounts");
	}
	
	@Configuration
	@EnableTransactionManagement(proxyTargetClass = true)
	static class ContextConfiguration {
		@Bean
		public BankTeller teller() {
			return new BankTeller();
		}
		@Bean
		public DataSource dataSource() {
			JdbcDataSource ds = new JdbcDataSource();
			ds.setURL("jdbc:h2:mem:db;DB_CLOSE_DELAY=-1");
			ds.setUser("sa");
			return new P6DataSource(ds);
		}
		@Bean
		public PlatformTransactionManager txManager() {
			return new DataSourceTransactionManager(dataSource());
		}
	}

}