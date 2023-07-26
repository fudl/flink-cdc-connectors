/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.sqlserver.source;

import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import com.ververica.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import com.ververica.cdc.connectors.sqlserver.source.dialect.SqlServerSchema;
import com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerJdbcConfiguration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/*
 * <sqlserver数据源> -  验证sqlserver数据源连接、指定实例
 *
 * @author fudl
 * @since  2023/7/24 14:03
 */
public class SqlServerSourceTest extends TestLogger {

	private static String SERVER_NAME = "";
	private static String HOST = "192.192.192.214";
	private static int PORT = 1433;
	private static String USER_NAME = "sa";
	private static String PASSWORD = "Test@admin!";
	private static String INSTANCE_NAME = "";
	private static String DB_NAME = "HIS_ORACLE_PRO";
	private static String DB_NAME2 = "ceshi";

	private static final Logger LOG = LoggerFactory.getLogger(SqlServerSourceTest.class);

	@Rule
	public final Timeout timeoutPerTest = Timeout.seconds(300);

	private transient SqlServerSchema sqlserverSchema;

	@Test
	public void testCommonDb() throws Exception {
		SqlServerSourceConfigFactory sourceConfigFactory = getConfigFactory();
		SqlServerSourceConfig sourceConfig = sourceConfigFactory.create(0);
		SqlServerSourceConfig sqlserverSourceConfig = (SqlServerSourceConfig) sourceConfig;
		SqlServerDialect sqlServerDialect = new SqlServerDialect(sourceConfigFactory);
		try (JdbcConnection jdbcConnection = SqlServerConnectionUtils.createSqlServerConnection(sourceConfig.getDbzConnectorConfig())) {
			List<TableId> list =  SqlServerConnectionUtils.listTables(
					jdbcConnection,
					sqlserverSourceConfig.getTableFilters(),
					sqlserverSourceConfig.getDatabaseList());
			list.forEach(x-> System.out.println(x.toString()));
		} catch (SQLException e) {
			throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
		}

	}

	@Test
	public void testSp() throws Exception {
	}

	public static SqlServerSourceConfigFactory getConfigFactory() {
		Properties p =new Properties();
		p.setProperty("database.instance","test");
		return (SqlServerSourceConfigFactory)
				new SqlServerSourceConfigFactory()
						.hostname(HOST)
						.port(PORT)
						.username(USER_NAME)
						.password(PASSWORD)
						.databaseList(DB_NAME)
						.debeziumProperties(p)
				;
	}

	@Test
	public void hostnameAndDefaultPortConnectionUrl() {
		final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
				defaultConfig()
						.with(SqlServerConnectorConfig.HOSTNAME, "example.com")
						.build());
		assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}:${port}");
	}

	@Test
	public void hostnameAndPortConnectionUrl() {
		final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
				defaultConfig()
						.with(SqlServerConnectorConfig.HOSTNAME, "example.com")
						.with(SqlServerConnectorConfig.PORT, "11433")
						.build());
		assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}:${port}");
	}

	@Test
	public void hostnameAndInstanceConnectionUrl() {
		final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
				defaultConfig()
						.with(SqlServerConnectorConfig.HOSTNAME, "example.com")
						.with(SqlServerConnectorConfig.INSTANCE, "instance")
						.with(SqlServerConnectorConfig.SERVER_NAME,"sqlserver")
						.build());
		assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}\\instance");
	}

	@Test
	public void hostnameAndInstanceAndPortConnectionUrl() {
		final SqlServerConnectorConfig connectorConfig = new SqlServerConnectorConfig(
				defaultConfig()
						.with(SqlServerConnectorConfig.HOSTNAME, "example.com")
						.with(SqlServerConnectorConfig.INSTANCE, "instance")
						.with(SqlServerConnectorConfig.PORT, "11433")
						.build());
		assertEquals(connectionUrl(connectorConfig), "jdbc:sqlserver://${hostname}\\instance:${port}");
	}

	private Configuration.Builder defaultConfig() {
		return Configuration.create()
				.with(SqlServerConnectorConfig.HOSTNAME, "localhost")
				.with(SqlServerConnectorConfig.USER, "debezium");
	}

	private String connectionUrl(SqlServerConnectorConfig connectorConfig) {
		SqlServerJdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
		return createUrlPattern(jdbcConfig, false);
	}

	protected static String createUrlPattern(SqlServerJdbcConfiguration config, boolean useSingleDatabase) {
		String pattern = "jdbc:sqlserver://${" + JdbcConfiguration.HOSTNAME + "}";
		if (config.getInstance() != null) {
			pattern += "\\" + config.getInstance();
			if (config.getPortAsString() != null) {
				pattern += ":${" + JdbcConfiguration.PORT + "}";
			}
		}
		else {
			pattern += ":${" + JdbcConfiguration.PORT + "}";
		}
		if (useSingleDatabase) {
			pattern += ";databaseName=${" + JdbcConfiguration.DATABASE + "}";
		}

		return pattern;
	}

}
