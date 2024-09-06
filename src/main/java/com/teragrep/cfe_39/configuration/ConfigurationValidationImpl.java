/*
 * HDFS Data Ingestion for PTH_06 use CFE-39
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.cfe_39.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class ConfigurationValidationImpl implements ConfigurationValidation {

    private final Logger LOGGER = LoggerFactory.getLogger(ConfigurationValidationImpl.class);
    private final Set<String> requiredKeys;

    public ConfigurationValidationImpl() {
        this.requiredKeys = new HashSet<>();
    }

    public void validate(Properties properties) {
        validateKeys(properties);
        validateValues(properties);
    }

    private void validateKeys(Properties properties) {
        if (requiredKeys.isEmpty()) {
            loadRequiredKeys();
        }
        int requiredCount = 0;
        for (Map.Entry keyValuePair : properties.entrySet()) {
            if (requiredKeys.contains(keyValuePair.getKey().toString())) {
                requiredCount++;
            }
            else {
                throw new IllegalStateException("Unauthorized key " + keyValuePair.getKey().toString());
            }
        }
        if (requiredCount < requiredKeys.size()) {
            for (String key : requiredKeys) {
                if (!properties.containsKey(key)) {
                    throw new IllegalStateException("Missing required key " + key);
                }
            }
        }
    }

    private void validateValues(Properties properties) {
        // Check the requirements for the specific key-value pairs.
        if (Long.parseLong(properties.getProperty("pruneOffset")) <= 0) {
            throw new IllegalArgumentException(
                    "pruneOffset must be set to >0, got " + properties.getProperty("pruneOffset")
            );
        }
        if (Long.parseLong(properties.getProperty("maximumFileSize")) <= 0) {
            throw new IllegalArgumentException(
                    "maximumFileSize must be set to >0, got " + properties.getProperty("maximumFileSize")
            );
        }
        if (Long.parseLong(properties.getProperty("numOfConsumers")) <= 0) {
            throw new IllegalArgumentException(
                    "numOfConsumers must be set to >0, got " + properties.getProperty("numOfConsumers")
            );
        }
        if (Long.parseLong(properties.getProperty("maximumFileSize")) <= 0) {
            throw new IllegalArgumentException(
                    "maximumFileSize must be set to >0, got " + properties.getProperty("maximumFileSize")
            );
        }
        if (Long.parseLong(properties.getProperty("consumerTimeout")) <= 0) {
            throw new IllegalArgumentException(
                    "consumerTimeout must be set to >0, got " + properties.getProperty("consumerTimeout")
            );
        }
    }

    private void loadRequiredKeys() {
        // Common
        requiredKeys.add("pruneOffset");
        requiredKeys.add("queueDirectory");
        requiredKeys.add("maximumFileSize");
        requiredKeys.add("queueTopicPattern");
        requiredKeys.add("numOfConsumers");
        requiredKeys.add("consumerTimeout");
        requiredKeys.add("skipNonRFC5424Records");
        requiredKeys.add("skipEmptyRFC5424Records");
        requiredKeys.add("log4j2.configurationFile");
        // kafka
        requiredKeys.add("java.security.auth.login.config");
        requiredKeys.add("bootstrap.servers");
        requiredKeys.add("auto.offset.reset");
        requiredKeys.add("enable.auto.commit");
        requiredKeys.add("group.id");
        requiredKeys.add("security.protocol");
        requiredKeys.add("sasl.mechanism");
        requiredKeys.add("max.poll.records");
        requiredKeys.add("fetch.max.bytes");
        requiredKeys.add("request.timeout.ms");
        requiredKeys.add("max.poll.interval.ms");
        requiredKeys.add("useMockKafkaConsumer");
        // HDFS
        requiredKeys.add("hdfsPath");
        requiredKeys.add("hdfsuri");
        requiredKeys.add("dfs.client.use.datanode.hostname");
        requiredKeys.add("dfs.data.transfer.protection");
        requiredKeys.add("dfs.encrypt.data.transfer.cipher.suites");
        // Kerberos
        requiredKeys.add("hadoop.security.authentication");
        requiredKeys.add("hadoop.security.authorization");
        requiredKeys.add("dfs.namenode.kerberos.principal.pattern");
        requiredKeys.add("java.security.krb5.kdc");
        requiredKeys.add("java.security.krb5.realm");
        requiredKeys.add("KerberosKeytabUser");
        requiredKeys.add("KerberosKeytabPath");
        requiredKeys.add("kerberosLoginAutorenewal");
    }

}
