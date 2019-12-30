package com.jdbc.connectionsAndTransactions.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

import javax.sql.DataSource;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.support.AnnotationConfigContextLoader;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.DefaultTransactionStatus;

import com.p6spy.engine.spy.P6DataSource;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:h2-test-db.properties")
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
class TransactionalProxyTests {
	
	@Autowired
	private BankTeller bankTeller;
	
	@Autowired
	private DataSource ds;
	
	@Autowired
	private PlatformTransactionManager txManager;
	
	private final long accountBalance = 100;
	
	@BeforeEach
	public void setUp() {
		try (Connection connection = ds.getConnection()) {
			createTables(connection);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		Mockito.clearInvocations(txManager);
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
	public void getAccountBalance_ForUser1_ShouldBeEqualTo100() {
		Long balance = bankTeller.getAccountBalance("User1");
		assertThat(balance).isEqualTo(accountBalance);
	}
	
	@Test
	public void nestedMethodCalledDirectly_WithPropagationRequiresNew_ShouldBeInTheSameTransaction() {
		bankTeller.callInternalMethodDirectly("User1");
		verify(txManager, times(1)).commit(new DefaultTransactionStatus(null, false, false, false, false, null));
	}
	@Test
	public void nestedMethodCalledFromProxy_WithPropagationRequiresNew_ShouldBeInSeparateTransaction() {
		bankTeller.callInternalMethodFromProxy("User1");
		verify(txManager, times(2)).commit(new DefaultTransactionStatus(null, false, false, false, false, null));
	}

	// For class containing method annotated with @Transactional proxy will be created
	static class BankTeller {
		@Autowired
		private BankTeller self;
		@Autowired
		private DataSource ds;
		
		// Propagation.REQUIRED is default option
		// Spring automatically open and commit/rollback a transaction. It also
		// makes sure that the JdbcTemplate and SimpleJdbcInsert transparently
		// use exactly the same connection/transaction
		@Transactional(propagation = Propagation.REQUIRED)
		public Long getAccountBalance(String name) {
			Long balance = new JdbcTemplate(ds).queryForObject(
					"select balance from accounts "
					+ "where name = ?", Long.class, name);
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("date_occurred", LocalDate.now());
			params.addValue("description", "Get Account Balance");
			params.addValue("name", name);
			new SimpleJdbcInsert(ds).withTableName("account_activity").execute(params);
			return balance;
		}
		
		@Transactional(propagation = Propagation.REQUIRED)
		public void callInternalMethodDirectly(String name) {
			new JdbcTemplate(ds).queryForObject(
	                "select balance from accounts where name = ?", Long.class, name);
			getAccountActivity(name);
		}
		@Transactional(propagation = Propagation.REQUIRED)
		public void callInternalMethodFromProxy(String name) {
			new JdbcTemplate(ds).queryForObject(
	                "select balance from accounts where name = ?", Long.class, name);
			self.getAccountActivity(name);
		}
		@Transactional(propagation = Propagation.REQUIRES_NEW)
		public List<String> getAccountActivity(String name) {
			List<String> activities = new JdbcTemplate(ds)
					.queryForList("select description from account_activity where name = ?", String.class, name);
			return activities;
		}
	}
	
	private void createTables(Connection conn) throws SQLException {
		conn.createStatement().execute("create table if not exists "
				+ "accounts (name varchar primary key, balance bigint)");
		conn.createStatement().execute("create table if not exists "
				+ "account_activity (date_occurred date, description VARCHAR,"
				+ " name varchar, foreign key (name) references accounts(name))");
		conn.createStatement().execute("insert into accounts values"
				+ "('User1'," + accountBalance + ")");
	}
	private void dropTables(Connection conn) throws SQLException {
		conn.createStatement().executeUpdate("drop table account_activity");
		conn.createStatement().executeUpdate("drop table accounts");
	}
	
	@Configuration
	// @EnableTransactionManagement enables creating proxy by spring
	// Spring AOP uses either JDK dynamic proxies or CGLIB to create the proxy for a given target object.
	// If the target object to be proxied implements at least one interface then a JDK dynamic proxy will be used.
	// If the target object does not implement any interfaces then a CGLIB proxy will be created.
	// In order to force subclass-based (CGLIB) proxies, set proxyTargetClass to true.
	// CGLIB proxy issues:
	// - final methods cannot be advised, as they cannot be overriden
	// - we need the CGLIB 2 binaries on your classpath
	// - the constructor of proxied object will be called twice
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
			return Mockito.spy(new DataSourceTransactionManager(dataSource()));
		}
	}

}