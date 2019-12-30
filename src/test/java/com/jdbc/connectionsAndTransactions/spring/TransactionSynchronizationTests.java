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
import org.mockito.InOrder;
import org.mockito.Mockito;
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
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.p6spy.engine.spy.P6DataSource;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:h2-test-db.properties")
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
class TransactionSynchronizationTests {
	
	@Autowired
	private BankTeller bankTeller;
	
	@Autowired
	private EmailService emailService;
	
	@Autowired
	private DataSource ds;
	
	@Autowired
	private PlatformTransactionManager txManager;
	
	@BeforeEach
	public void setUp() {
		try (Connection connection = ds.getConnection()) {
			createTables(connection);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		Mockito.clearInvocations(txManager);
		Mockito.clearInvocations(emailService);
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
	public void sendEmail_ShouldBeInvokedAfterTransactionCommit() throws SQLException {
		Long balance = bankTeller.getAccountBalance("User1");
		InOrder inOrder = Mockito.inOrder(txManager, emailService);
		inOrder.verify(txManager).commit(new DefaultTransactionStatus(null, false, false, false, false, null));
	    inOrder.verify(emailService).sendEmail("User1");
		assertThat(balance).isEqualTo(100);
	}
	
	static class BankTeller {
		@Autowired
		private DataSource ds;
		@Autowired
		private EmailService emailService;
		
		@Transactional
		public Long getAccountBalance(String name) throws SQLException {
			TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
				@Override
				public void afterCommit() {
					emailService.sendEmail(name);
				}
			});
			Long balance = new JdbcTemplate(ds).queryForObject(
				"select balance from accounts " +
				"where name = ?", Long.class, name);
			return balance;
		}
	}
	
	static class EmailService {
		public void sendEmail(String name) {
			System.out.println("Sending email to " + name);
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
		public EmailService emailService() {
			return Mockito.spy(new EmailService());
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
			return Mockito.spy(new DataSourceTransactionManager(dataSource()));
		}
	}

}