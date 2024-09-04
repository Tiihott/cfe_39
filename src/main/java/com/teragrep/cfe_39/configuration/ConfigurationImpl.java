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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public final class ConfigurationImpl implements Configuration {

    private final Logger LOGGER = LoggerFactory.getLogger(ConfigurationImpl.class);
    private final Properties properties;

    public ConfigurationImpl() {
        this(new Properties());
    }

    public ConfigurationImpl(Properties properties) {
        this.properties = properties;
    }

    @Override
    public ConfigurationImpl loadPropertiesFile() throws IOException {
        // Maybe implement the configuration validation here instead of a public method?
        final Properties newProperties = new Properties();
        Path configPath = Paths
                .get(System.getProperty("cfe_39.config.location", "/opt/teragrep/cfe_39/etc/application.properties"));
        LOGGER.info("Loading application config <[{}]>", configPath.toAbsolutePath());
        try (InputStream inputStream = Files.newInputStream(configPath)) {
            newProperties.load(inputStream);
            LOGGER.debug("Got configuration: <{}>", newProperties);
        }
        return new ConfigurationImpl(newProperties);
    }

    @Override
    public ConfigurationImpl with(String key, String value) {
        // Maybe implement the configuration validation here instead of a public method?
        final Properties newProperties = new Properties(properties);
        newProperties.setProperty(key, value);
        return new ConfigurationImpl(newProperties);
    }

    @Override
    public String valueOf(String key) {
        if (has(key)) {
            return properties.getProperty(key);
        }
        throw new IllegalArgumentException("Key not found: " + key);
    }

    @Override
    public boolean has(String key) {
        return properties.containsKey(key);
    }
}
