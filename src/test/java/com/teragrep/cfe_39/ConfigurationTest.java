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
package com.teragrep.cfe_39;

import com.teragrep.cfe_39.configuration.ConfigurationImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class ConfigurationTest {

    private final Logger LOGGER = LoggerFactory.getLogger(ConfigurationTest.class);

    @Test
    public void kafkaPropertiesConfigurationTest() {
        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            ConfigurationImpl configuration = new ConfigurationImpl().loadPropertiesFile();
            Properties readerKafkaProperties = configuration.toKafkaConsumerProperties();
            // Test extracting useMockKafkaConsumer value from config.
            boolean useMockKafkaConsumer = Boolean
                    .parseBoolean(readerKafkaProperties.getProperty("useMockKafkaConsumer", "false"));
            Assertions.assertTrue(useMockKafkaConsumer);
            LOGGER.debug("useMockKafkaConsumer: {}", useMockKafkaConsumer);
        });
    }

    @Test
    public void brokenConfigurationTest() {
        // Set system properties to use the broken configuration.
        System
                .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/broken.application.properties");
        Exception e = Assertions.assertThrows(Exception.class, () -> {
            ConfigurationImpl configuration = new ConfigurationImpl().loadPropertiesFile();
        });
        Assertions.assertEquals("Missing required key hdfsuri", e.getMessage());
    }

    @Test
    public void configurationEqualityTest() {
        // Set system properties to use the valid configuration.
        System
                .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
        assertDoesNotThrow(() -> {
            ConfigurationImpl configuration1 = new ConfigurationImpl().loadPropertiesFile();
            ConfigurationImpl configuration2 = new ConfigurationImpl().loadPropertiesFile();
            ConfigurationImpl configuration3 = new ConfigurationImpl().loadPropertiesFile();
            ConfigurationImpl configuration4 = new ConfigurationImpl().loadPropertiesFile();
            Assertions.assertNotEquals(configuration1, configuration2);
            Assertions.assertNotEquals(configuration1, configuration3);
            Assertions.assertNotEquals(configuration3, configuration4);
            configuration3 = configuration3.with("hdfsuri", "12345");
            configuration4 = configuration4.with("hdfsuri", "12345");
            Assertions.assertNotEquals(configuration1, configuration3);
            Assertions.assertNotEquals(configuration3, configuration4);
        });
    }

    @Test
    public void configurationWithTest() {
        // Set system properties to use the valid configuration.
        System
                .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
        assertDoesNotThrow(() -> {
            ConfigurationImpl configuration1 = new ConfigurationImpl().loadPropertiesFile();
            ConfigurationImpl configuration2 = new ConfigurationImpl().loadPropertiesFile().with("hdfsuri", "12345");
            Assertions.assertEquals(configuration1.valueOf("hdfsuri"), "hdfs://localhost:45937/");
            Assertions.assertEquals(configuration2.valueOf("hdfsuri"), "12345");
        });
    }

    @Test
    public void configurationWithFailTest() {
        // Set system properties to use the valid configuration.
        System
                .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
        Exception e = Assertions.assertThrows(IllegalStateException.class, () -> {
            ConfigurationImpl configuration = new ConfigurationImpl()
                    .loadPropertiesFile()
                    .with("unauthorized_key", "12345");
        });
        Assertions.assertEquals("Unauthorized key unauthorized_key", e.getMessage());
    }

    @Test
    public void configurationWithFailTest2() {
        // Set system properties to use the valid configuration.
        System
                .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
        Exception e = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            ConfigurationImpl configuration = new ConfigurationImpl().loadPropertiesFile().with("maximumFileSize", "0");
        });
        Assertions.assertEquals("maximumFileSize must be set to >0, got 0", e.getMessage());
    }

}
