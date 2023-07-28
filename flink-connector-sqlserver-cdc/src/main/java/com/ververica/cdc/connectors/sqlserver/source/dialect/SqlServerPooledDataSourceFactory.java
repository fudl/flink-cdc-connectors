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

package com.ververica.cdc.connectors.sqlserver.source.dialect;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;

import static com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils.DATABASE_INSTANCE_KEY;
import static com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils.URL_INSTANCE_PATTERN;

/** Factory to create {@link JdbcConnectionPoolFactory} for SQL Server. */
public class SqlServerPooledDataSourceFactory extends JdbcConnectionPoolFactory {

    private static final String URL_PATTERN = "jdbc:sqlserver://%s:%s;databaseName=%s";

    @Override
    public String getJdbcUrl(JdbcSourceConfig sourceConfig) {
        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();
        String database = sourceConfig.getDatabaseList().get(0);
        String instanceName = sourceConfig.getDbzProperties().getProperty(DATABASE_INSTANCE_KEY);

        if (!Strings.isNullOrEmpty(instanceName)) {
            return String.format(URL_INSTANCE_PATTERN, hostName, instanceName, database);
        }
        return String.format(URL_PATTERN, hostName, port, database);
    }
}
