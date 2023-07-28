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

package com.ververica.cdc.connectors.sqlserver;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava30.com.google.common.base.Strings;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.ververica.cdc.debezium.Validator;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import static com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils.DATABASE_INSTANCE_KEY;
import static com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils.URL_INSTANCE_PATTERN;
import static com.ververica.cdc.connectors.sqlserver.source.utils.SqlServerConnectionUtils.URL_PATTERN;

/**
 * Validator for SqlServer to validate: SqlServer CDC mechanism is enabled or not, SqlServer version
 * is supported or not.
 */
public class SqlServerValidator implements Validator {

    private static final long serialVersionUID = 1L;
    private final Properties properties;

    public SqlServerValidator(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void validate() {
        try (Connection connection = openConnection(properties);
                PreparedStatement preparedStatement =
                        connection.prepareStatement(
                                "select 1 from sys.databases where name= ? AND is_cdc_enabled=1")) {
            checkVersion(connection);
            checkCdcEnabled(preparedStatement);
        } catch (SQLException ex) {
            throw new TableException(
                    "Unexpected error while connecting to SqlServer and validating", ex);
        }
    }

    private void checkCdcEnabled(PreparedStatement preparedStatement) throws SQLException {
        String dbname = properties.getProperty("database.dbname");
        preparedStatement.setString(1, dbname);
        if (!preparedStatement.executeQuery().next()) {
            throw new ValidationException(
                    String.format("SqlServer database %s do not enable cdc.", dbname));
        }
    }

    private void checkVersion(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        // For more information on sqlserver version, please refer to
        // https://docs.microsoft.com/en-us/troubleshoot/sql/general/determine-version-edition-update-level.
        if (metaData.getDatabaseMajorVersion() < 11) {
            throw new ValidationException(
                    String.format(
                            "Currently Flink SqlServer CDC connector only supports SqlServer "
                                    + "whose version is larger or equal to 11, but actual is %d.",
                            metaData.getDatabaseMajorVersion()));
        }
    }

    public static Connection openConnection(Properties properties) throws SQLException {
        DriverManager.registerDriver(new SQLServerDriver());
        String hostname = properties.getProperty("database.hostname");
        String port = properties.getProperty("database.port");
        String dbname = properties.getProperty("database.dbname");
        String userName = properties.getProperty("database.user");
        String userpwd = properties.getProperty("database.password");
        String instanceName = properties.getProperty(DATABASE_INSTANCE_KEY);

        String url = String.format(URL_PATTERN, hostname, port, dbname);
        if (!Strings.isNullOrEmpty(instanceName)) {
            url = String.format(URL_INSTANCE_PATTERN, hostname, instanceName, dbname);
        }

        return DriverManager.getConnection(url, userName, userpwd);
    }
}
