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

import org.apache.logging.log4j.core.config.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class KafkaConfiguration {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaConfiguration.class);

    private final Map<String, String> config;

    public KafkaConfiguration(Map<String, String> config) {
        this.config = config;
    }

    // printers for the configuration parameters.

    public String javaSecurityAuthLoginConfig() {
        final String javaSecurityAuthLoginConfig = config.get("java.security.auth.login.config");
        if (javaSecurityAuthLoginConfig == null) {
            throw new ConfigurationException("Configuration error. <java.security.auth.login.config> must be set.");
        }
        else {
            return javaSecurityAuthLoginConfig;
        }
    }

    public String bootstrapServers() {
        final String bootstrapServers = config.get("bootstrap.servers");
        if (bootstrapServers == null) {
            throw new ConfigurationException("Configuration error. <bootstrap.servers> must be set.");
        }
        else {
            return bootstrapServers;
        }
    }

    public String autoOffsetReset() {
        final String autoOffsetReset = config.get("auto.offset.reset");
        if (autoOffsetReset == null) {
            throw new ConfigurationException("Configuration error. <auto.offset.reset> must be set.");
        }
        else {
            return autoOffsetReset;
        }
    }

    public String enableAutoCommit() {
        final String enableAutoCommit = config.get("enable.auto.commit");
        if (enableAutoCommit == null) {
            throw new ConfigurationException("Configuration error. <enable.auto.commit> must be set.");
        }
        else {
            return enableAutoCommit;
        }
    }

    public String groupId() {
        final String groupId = config.get("group.id");
        if (groupId == null) {
            throw new ConfigurationException("Configuration error. <group.id> must be set.");
        }
        else {
            return groupId;
        }
    }

    public String securityProtocol() {
        final String securityProtocol = config.get("security.protocol");
        if (securityProtocol == null) {
            throw new ConfigurationException("Configuration error. <security.protocol> must be set.");
        }
        else {
            return securityProtocol;
        }
    }

    public String saslMechanism() {
        final String saslMechanism = config.get("sasl.mechanism");
        if (saslMechanism == null) {
            throw new ConfigurationException("Configuration error. <sasl.mechanism> must be set.");
        }
        else {
            return saslMechanism;
        }
    }

    public long maxPollRecords() {
        final String numString = config.get("max.poll.records");
        if (numString == null) {
            throw new ConfigurationException("Configuration error. <max.poll.records> must be set.");
        }
        else {
            final long maxPollRecords;
            try {
                maxPollRecords = Long.parseLong(numString);
            }
            catch (NumberFormatException e) {
                LOGGER.error("Configuration error. Invalid value for <max.poll.records>: <{}>", e.getMessage());
                throw new RuntimeException(e);
            }
            if (maxPollRecords < 0) {
                throw new ConfigurationException("Configuration error. <max.poll.records> must be a positive value.");
            }
            else {
                return maxPollRecords;
            }
        }
    }

    public long fetchMaxBytes() {
        final String numString = config.get("fetch.max.bytes");
        if (numString == null) {
            throw new ConfigurationException("Configuration error. <fetch.max.bytes> must be set.");
        }
        else {
            final long fetchMaxBytes;
            try {
                fetchMaxBytes = Long.parseLong(numString);
            }
            catch (NumberFormatException e) {
                LOGGER.error("Configuration error. Invalid value for <fetch.max.bytes>: <{}>", e.getMessage());
                throw new RuntimeException(e);
            }
            if (fetchMaxBytes < 0) {
                throw new ConfigurationException("Configuration error. <fetch.max.bytes> must be a positive value.");
            }
            else {
                return fetchMaxBytes;
            }
        }
    }

    public long requestTimeoutMs() {
        final String numString = config.get("request.timeout.ms");
        if (numString == null) {
            throw new ConfigurationException("Configuration error. <request.timeout.ms> must be set.");
        }
        else {
            final long requestTimeoutMs;
            try {
                requestTimeoutMs = Long.parseLong(numString);
            }
            catch (NumberFormatException e) {
                LOGGER.error("Configuration error. Invalid value for <request.timeout.ms>: <{}>", e.getMessage());
                throw new RuntimeException(e);
            }
            if (requestTimeoutMs < 0) {
                throw new ConfigurationException("Configuration error. <request.timeout.ms> must be a positive value.");
            }
            else {
                return requestTimeoutMs;
            }
        }
    }

    public long maxPollIntervalMs() {
        final String numString = config.get("max.poll.interval.ms");
        if (numString == null) {
            throw new ConfigurationException("Configuration error. <max.poll.interval.ms> must be set.");
        }
        else {
            final long maxPollIntervalMs;
            try {
                maxPollIntervalMs = Long.parseLong(numString);
            }
            catch (NumberFormatException e) {
                LOGGER.error("Configuration error. Invalid value for <max.poll.interval.ms>: <{}>", e.getMessage());
                throw new RuntimeException(e);
            }
            if (maxPollIntervalMs < 0) {
                throw new ConfigurationException("Configuration error. <request.timeout.ms> must be a positive value.");
            }
            else {
                return maxPollIntervalMs;
            }
        }
    }

    public boolean useMockKafkaConsumer() {
        final String useMockKafkaConsumer = config.get("useMockKafkaConsumer");
        if (useMockKafkaConsumer == null) {
            throw new ConfigurationException("Configuration error. <useMockKafkaConsumer> must be set.");
        }
        else {
            return Boolean.parseBoolean(useMockKafkaConsumer);
        }
    }

}
