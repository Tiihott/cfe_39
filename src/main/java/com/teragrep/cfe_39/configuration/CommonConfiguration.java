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

public final class CommonConfiguration {

    private final Logger LOGGER = LoggerFactory.getLogger(CommonConfiguration.class);

    private final Map<String, String> config;

    public CommonConfiguration(Map<String, String> map) {
        this.config = map;
    }

    // printers for configuration file paths.

    public String egressConfigurationFile() {
        return config
                .getOrDefault("egress.configurationFile", System.getProperty("user.dir") + "/rpm/resources/egress.properties");
    }

    public String ingressConfigurationFile() {
        return config
                .getOrDefault("ingress.configurationFile", System.getProperty("user.dir") + "/rpm/resources/ingress.properties");
    }

    public String log4j2ConfigurationFile() {
        return config
                .getOrDefault("log4j2.configurationFile", System.getProperty("user.dir") + "/rpm/resources/log4j2.properties");
    }

    // printers for the configuration parameters.

    public String queueTopicPattern() {
        return config.getOrDefault("queueTopicPattern", ".*");
    }

    public String queueDirectory() {
        return config.getOrDefault("queueDirectory", System.getProperty("user.dir") + "/rpm/resources/queue");
    }

    public boolean skipNonRFC5424Records() {
        final String skipString = config.get("skipNonRFC5424Records");
        if (skipString == null) {
            throw new ConfigurationException("Configuration error. <skipNonRFC5424Records> must be set.");
        }
        else {
            return Boolean.parseBoolean(skipString);
        }
    }

    public boolean skipEmptyRFC5424Records() {
        final String skipString = config.get("skipEmptyRFC5424Records");
        if (skipString == null) {
            throw new ConfigurationException("Configuration error. <skipEmptyRFC5424Records> must be set.");
        }
        else {
            return Boolean.parseBoolean(skipString);
        }
    }

    public long consumerTimeout() {
        final String consumerTimeoutString = config.get("consumerTimeout");
        if (consumerTimeoutString == null) {
            throw new ConfigurationException("Configuration error. <consumerTimeout> must be set.");
        }
        else {
            final long consumerTimeout;
            try {
                consumerTimeout = Long.parseLong(consumerTimeoutString);
            }
            catch (NumberFormatException e) {
                throw new RuntimeException(e);
            }
            if (consumerTimeout <= 0) {
                throw new ConfigurationException(
                        "Configuration error. <consumerTimeout> must be a positive long value."
                );
            }
            else {
                return consumerTimeout;
            }
        }
    }

}
