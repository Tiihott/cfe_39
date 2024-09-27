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

// This class will only hold the common configuration parameters. Rename to CommonConfiguration?
public final class ConfigurationImpl implements Configuration {

    private final Logger LOGGER = LoggerFactory.getLogger(ConfigurationImpl.class);

    private final Properties properties;
    private final ConfigurationValidationImpl configurationValidationImpl;
    private final Configuration hdfsConfiguration;
    private final Configuration kafkaConfiguration;

    public ConfigurationImpl() {
        this(new Properties(), new HdfsConfiguration(), new KafkaConfiguration());
    }

    public ConfigurationImpl(
            Properties properties,
            HdfsConfiguration hdfsConfiguration,
            KafkaConfiguration kafkaConfiguration
    ) {
        this.properties = properties;
        this.hdfsConfiguration = hdfsConfiguration; // Initializes HdfsConfiguration
        this.kafkaConfiguration = kafkaConfiguration; // Initializes KafkaConfiguration
        this.configurationValidationImpl = new ConfigurationValidationImpl();
    }

    // This method should load the common properties belonging to this configuration object, but it should also ask the other configuration objects to do the same.
    @Override
    public void loadPropertiesFile(String configurationFile) throws IOException {
        Path configPath = Paths.get(configurationFile);
        LOGGER.info("Loading application config <[{}]>", configPath.toAbsolutePath());
        try (InputStream inputStream = Files.newInputStream(configPath)) {
            properties.load(inputStream);
            LOGGER.debug("Got configuration: <{}>", properties);
            configurationValidationImpl.validate(properties);
        }
        // also load the hdfs and kafka configuration files.
        hdfsConfiguration
                .loadPropertiesFile(properties.getProperty("egress.configurationFile", System.getProperty("user.dir") + "/rpm/resources/egress.properties"));
        kafkaConfiguration
                .loadPropertiesFile(properties.getProperty("ingress.configurationFile", System.getProperty("user.dir") + "/rpm/resources/ingress.properties"));
        configureLogging();
    }

    // Used only during testing to change existing property values, make a fake for this.
    @Override
    public void with(String key, String value) {
        if (this.has(key)) {
            properties.setProperty(key, value);
            configurationValidationImpl.validate(properties);
        }
        else if (hdfsConfiguration.has(key)) {
            hdfsConfiguration.with(key, value);
        }
        else if (kafkaConfiguration.has(key)) {
            kafkaConfiguration.with(key, value);
        }
        else {
            throw new IllegalArgumentException("Key not found: " + key);
        }
    }

    @Override
    public String valueOf(String key) {
        if (this.has(key)) {
            return properties.getProperty(key);
        }
        if (kafkaConfiguration.has(key)) {
            return kafkaConfiguration.valueOf(key);
        }
        if (hdfsConfiguration.has(key)) {
            return hdfsConfiguration.valueOf(key);
        }
        throw new IllegalArgumentException("Key not found: " + key);
    }

    @Override
    public boolean has(String key) {
        return properties.containsKey(key);
    }

    private void configureLogging() throws IOException {
        // Just for loggers to work
        Path log4j2Config = Paths
                .get(properties.getProperty("log4j2.configurationFile", System.getProperty("user.dir") + "/rpm/resources/log4j2.properties"));
        LOGGER.info("Loading log4j2 config from <[{}]>", log4j2Config.toRealPath());
        Configurator.reconfigure(log4j2Config.toUri());
    }

}
