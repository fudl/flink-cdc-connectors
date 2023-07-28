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

import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import com.google.common.base.Strings;
import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import com.ververica.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import com.ververica.cdc.connectors.sqlserver.source.dialect.SqlServerSchema;
import com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils;
import io.debezium.config.Configuration;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerJdbcConfiguration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/*
 * <sqlserver数据源> -  验证sqlserver数据源连接、指定实例
 *
 * @author fudl
 * @since  2023/7/24 14:03
 */
public class SqlServerSourceTest extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerSourceTest.class);
    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);
    private transient SqlServerSchema sqlserverSchema;

    // 测试配置是否连接数据库
    @Test
    public void testDbBySqlServerSourceConfigFactory() throws Exception {
        SqlServerSourceConfigFactory sourceConfigFactory = getConfigFactory();
        SqlServerSourceConfig sourceConfig = sourceConfigFactory.create(0);
        SqlServerSourceConfig sqlserverSourceConfig = (SqlServerSourceConfig) sourceConfig;
        try (JdbcConnection jdbcConnection =
                SqlServerConnectionUtils.createSqlServerConnection(
                        sourceConfig.getDbzConnectorConfig())) {
            List<TableId> list =
                    SqlServerConnectionUtils.listTables(
                            jdbcConnection,
                            sqlserverSourceConfig.getTableFilters(),
                            sqlserverSourceConfig.getDatabaseList());
            list.forEach(x -> System.out.println(x.toString()));
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Error to discover tables: " + e.getMessage(), e);
        }
    }

    @Test
    public void testDbBySqlServerSourceBuilder() throws Exception {
        SqlServerSourceBuilder.SqlServerIncrementalSource<String> sqlServerSource =
                generatorSqlServerSourceBuilder(defaultConfig().build()).build();
        // SourceFunction<String> sourceFunction = SqlServerSource.<String>builder().build();

    }

    @Test
    public void testConnectionUrl() {
        String url = connectionUrl(new SqlServerConnectorConfig(defaultConfig().build()));
        LOG.info("url={}", url);
    }

    public static SqlServerSourceConfigFactory getConfigFactory() {
        return generatorConfigFactory(defaultConfig().build());
    }

    // 获取 SqlServerSourceConfigFactory对象
    private static SqlServerSourceConfigFactory generatorConfigFactory(Configuration config) {
        Properties p = new Properties();
        // 判断是否使用instance
        if (!Strings.isNullOrEmpty(config.getString(SqlServerConnectorConfig.INSTANCE))) {
            p.setProperty("database.instance", config.getString(SqlServerConnectorConfig.INSTANCE));
        }
        return (SqlServerSourceConfigFactory)
                new SqlServerSourceConfigFactory()
                        .hostname(config.getString(SqlServerConnectorConfig.HOSTNAME))
                        .port(config.getInteger(SqlServerConnectorConfig.PORT))
                        .username(config.getString(SqlServerConnectorConfig.USER))
                        .password(config.getString(SqlServerConnectorConfig.PASSWORD))
                        .databaseList(config.getString(SqlServerConnectorConfig.DATABASE_NAME))
                        .debeziumProperties(p);
    }

    // 获取 SqlServerSourceBuilder对象
    private static SqlServerSourceBuilder generatorSqlServerSourceBuilder(Configuration config) {
        Properties p = new Properties();
        // 判断是否使用instance
        if (!Strings.isNullOrEmpty(config.getString(SqlServerConnectorConfig.INSTANCE))) {
            p.setProperty("database.instance", config.getString(SqlServerConnectorConfig.INSTANCE));
        }
        return new SqlServerSourceBuilder()
                .hostname(config.getString(SqlServerConnectorConfig.HOSTNAME))
                .port(config.getInteger(SqlServerConnectorConfig.PORT))
                .username(config.getString(SqlServerConnectorConfig.USER))
                .password(config.getString(SqlServerConnectorConfig.PASSWORD))
                .databaseList(config.getString(SqlServerConnectorConfig.DATABASE_NAME))
                .debeziumProperties(p);
    }

    // 构建默认数据库配置
    private static Configuration.Builder defaultConfig() {
        return Configuration.create()
                .with(SqlServerConnectorConfig.HOSTNAME, "192.192.192.214")
                .with(SqlServerConnectorConfig.INSTANCE, "TEST") // 如果指定实例名，则端口必须设置为0
                .with(SqlServerConnectorConfig.PORT, "0")
                .with(SqlServerConnectorConfig.SERVER_NAME, "")
                .with(SqlServerConnectorConfig.USER, "sa")
                .with(SqlServerConnectorConfig.PASSWORD, "Test@admin!")
                .with(SqlServerConnectorConfig.DATABASE_NAME, "HIS_ORACLE_PRO");
    }

    private String connectionUrl(SqlServerConnectorConfig connectorConfig) {
        SqlServerJdbcConfiguration jdbcConfig = connectorConfig.getJdbcConfig();
        return createUrlPattern(jdbcConfig, true);
    }

    // protected static String createUrlPattern(SqlServerJdbcConfiguration config, boolean
    // useSingleDatabase) {
    // 	String pattern = "jdbc:sqlserver://${" + JdbcConfiguration.HOSTNAME + "}";
    // 	if (config.getInstance() != null) {
    // 		pattern += "\\" + config.getInstance();
    // 		if (config.getPortAsString() != null) {
    // 			pattern += ":${" + JdbcConfiguration.PORT + "}";
    // 		}
    // 	} else {
    // 		pattern += ":${" + JdbcConfiguration.PORT + "}";
    // 	}
    // 	if (useSingleDatabase) {
    // 		pattern += ";databaseName=${" + JdbcConfiguration.DATABASE + "}";
    // 	}
    //
    // 	return pattern;
    // }

    // public static final String URL_INSTANCE_PATTERN =
    // "jdbc:sqlserver://%s;instanceName=%s;databaseName=%s";
    protected static String createUrlPattern(
            SqlServerJdbcConfiguration config, boolean useSingleDatabase) {
        String pattern = "jdbc:sqlserver://${" + JdbcConfiguration.HOSTNAME + "}";
        if (config.getInstance() != null) {
            pattern += ";instanceName=" + config.getInstance();
        } else {
            pattern += ":${" + JdbcConfiguration.PORT + "}";
        }
        if (useSingleDatabase) {
            pattern += ";databaseName=${" + JdbcConfiguration.DATABASE + "}";
        }

        return pattern;
    }
}
