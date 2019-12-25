package com.jdbc.connectionsAndTransactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.TestPropertySource;

import com.jdbc.connectionsAndTransactions.util.Util;

@DataJpaTest
@TestPropertySource("classpath:h2-test-db.properties")
class OpenConnectionTests {

	@Test
	void getConnection_ShouldOpenOneConnection() {
		try (Connection conn = DriverManager.getConnection(Util.connectionUrl)) {
			printConnectionInformation(conn);
			int connectionsCount = Util.getRowsCountFromTable(conn, "information_schema.sessions");
			assertThat(connectionsCount).isEqualTo(1);
			assertTrue(conn.isValid(0));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void printConnectionInformation(Connection conn) {
		List<Map<String, String>> connectionInformation = Util.getConnectionInformation(conn);
		String format = "%-5s\t| %-15s\t| %-15s\t| %-15s\t| %-20s\t| %s";
		System.out.println("Connection information:");
		System.out.println(String.format(format, "ID", "SESSION_START", "ISOLATION_LEVEL", "STATEMENT_START", "CONTAINS_UNCOMMITTED", "STATE"));
		for (Map<String, String> map : connectionInformation) {
			System.out.println(String.format(format,
					map.get("ID"), map.get("SESSION_START"), map.get("ISOLATION_LEVEL"),
					map.get("STATEMENT_START"), map.get("CONTAINS_UNCOMMITTED"),
					map.get("STATE")));
		}
	}

}
