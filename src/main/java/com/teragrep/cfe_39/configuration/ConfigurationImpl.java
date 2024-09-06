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

import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class ConfigurationImpl implements Configuration {

    private final Logger LOGGER = LoggerFactory.getLogger(ConfigurationImpl.class);
    private final Properties properties;
    private final ConfigurationValidationImpl configurationValidationImpl;

    public ConfigurationImpl() {
        this(new Properties());
    }

    public ConfigurationImpl(Properties properties) {
        this.properties = properties;
        configurationValidationImpl = new ConfigurationValidationImpl();
    }

    @Override
    public ConfigurationImpl loadPropertiesFile() throws IOException {
        final Properties newProperties = new Properties();
        Path configPath = Paths
                .get(System.getProperty("cfe_39.config.location", "/opt/teragrep/cfe_39/etc/application.properties"));
        LOGGER.info("Loading application config <[{}]>", configPath.toAbsolutePath());
        try (InputStream inputStream = Files.newInputStream(configPath)) {
            newProperties.load(inputStream);
            LOGGER.debug("Got configuration: <{}>", newProperties);
        }
        configurationValidationImpl.validate(newProperties);
        return new ConfigurationImpl(newProperties);
    }

    @Override
    public ConfigurationImpl with(String key, String value) {
        final Properties newProperties = new Properties();
        newProperties.putAll(properties);
        newProperties.setProperty(key, value);
        configurationValidationImpl.validate(newProperties);
        return new ConfigurationImpl(newProperties);
    }

    @Override
    public String valueOf(String key) {
        if (properties.containsKey(key)) {
            return properties.getProperty(key);
        }
        throw new IllegalArgumentException("Key not found: " + key);
    }

    @Override
    public Properties toKafkaConsumerProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", valueOf("bootstrap.servers"));
        kafkaProperties.put("auto.offset.reset", valueOf("auto.offset.reset"));
        kafkaProperties.put("enable.auto.commit", valueOf("enable.auto.commit"));
        kafkaProperties.put("group.id", valueOf("group.id"));
        kafkaProperties.put("security.protocol", valueOf("security.protocol"));
        kafkaProperties.put("sasl.mechanism", valueOf("sasl.mechanism"));
        kafkaProperties.put("max.poll.records", valueOf("max.poll.records"));
        kafkaProperties.put("fetch.max.bytes", valueOf("fetch.max.bytes"));
        kafkaProperties.put("request.timeout.ms", valueOf("request.timeout.ms"));
        kafkaProperties.put("max.poll.interval.ms", valueOf("max.poll.interval.ms"));
        kafkaProperties.put("useMockKafkaConsumer", valueOf("useMockKafkaConsumer"));
        return kafkaProperties;
    }

    @Override
    public void configureLogging() throws IOException {
        // Just for loggers to work
        Path log4j2Config = Paths
                .get(properties.getProperty("log4j2.configurationFile", System.getProperty("user.dir") + "/rpm/resources/log4j2.properties"));
        LOGGER.info("Loading log4j2 config from <[{}]>", log4j2Config.toRealPath());
        Configurator.reconfigure(log4j2Config.toUri());
    }

}
