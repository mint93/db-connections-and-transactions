package com.jdbc.connectionsAndTransactions.spring;

import static org.assertj.core.api.Assertions.assertThat;

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

import com.p6spy.engine.spy.P6DataSource;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:h2-test-db.properties")
@ContextConfiguration(loader=AnnotationConfigContextLoader.class)
class TransactionalPropagationNestededTests {
	
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
	public void audit_WithPropagationNested_DoesNotThrowException_ThenDataIsNotRollbacked() throws SQLException {
		boolean rollbackAudits = false;
		Long balance = bankTeller.updateAccountBalance("User1", 100, rollbackAudits);
		List<String> accountActivity = bankTeller.getAccountActivity("User1");
		assertThat(balance).isEqualTo(200);
		assertThat(accountActivity.size()).isEqualTo(2);
	}
	@Test
	public void audit_WithPropagationNested_ThrowsException_ThenOnlyAuditDataIsRollbacked() throws SQLException {
		boolean rollbackAudits = true;
		Long balance = bankTeller.updateAccountBalance("User1", 100, rollbackAudits);
		List<String> accountActivity = bankTeller.getAccountActivity("User1");
		assertThat(balance).isEqualTo(200);
		assertThat(accountActivity.size()).isZero();
	}
	
	static class BankTeller {
		@Autowired
		private BankTeller self;
		@Autowired
		private DataSource ds;
		
		@Transactional(propagation = Propagation.REQUIRED)
		public Long updateAccountBalance(String name, long amount, boolean rollbackAudit) {
			Long currentBalance = new JdbcTemplate(ds).queryForObject(
				"select balance from accounts " +
				"where name = ?", Long.class, name);
			// exception must be caught to prevent rollbacking whole transaction
			try {
				self.audit(name, "Get Account Balance", rollbackAudit);				
			} catch (Exception e) {
				System.out.println("Audit rollback");
			}
			if (currentBalance + amount < 0) {
				throw new IllegalArgumentException();
			}
			new JdbcTemplate(ds).update(
					"update accounts set balance = ?" +
					"where name = ?", currentBalance+amount, name);
			try {
				self.audit(name, "Update Account Balance", rollbackAudit);
			} catch (Exception e) {
				System.out.println("Audit rollback");
			}
			return new JdbcTemplate(ds).queryForObject(
					"select balance from accounts " +
							"where name = ?", Long.class, name);
		}
		
		// For NESTED propagation, Spring checks if a transaction exists, then if yes,
		// it marks a savepoint. This means if our business logic execution throws
		// an exception, then transaction rollbacks to this savepoint. If there's
		// no active transaction, it works like REQUIRED.
		@Transactional(propagation = Propagation.NESTED)
		public void audit(final String name, final String activity, boolean rollback) {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("name", name);
			params.addValue("date_occurred", LocalDate.now());
			params.addValue("description", activity);
			new SimpleJdbcInsert(ds)
				.withTableName("account_activity")
				.execute(params);
			if (rollback) {				
				throw new RuntimeException();
			}
		}
		
		@Transactional(propagation = Propagation.REQUIRED)
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