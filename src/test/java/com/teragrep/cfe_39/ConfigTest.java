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

import com.teragrep.cfe_39.configuration.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class ConfigTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigTest.class);

    @Test
    public void validConfigTest() {
        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            Config config = new Config();
            Properties readerKafkaProperties = config.getKafkaConsumerProperties();
            // Test extracting useMockKafkaConsumer value from config.
            boolean useMockKafkaConsumer = Boolean
                    .parseBoolean(readerKafkaProperties.getProperty("useMockKafkaConsumer", "false"));
            Assertions.assertTrue(useMockKafkaConsumer);
            LOGGER.debug("useMockKafkaConsumer: {}", useMockKafkaConsumer);
        });
    }

    @Test
    public void brokenConfigTest() {
        // Set system properties to use the broken configuration.
        System
                .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/broken.application.properties");
        // Test if the broken configuration throws the expected exception.
        Exception e = Assertions.assertThrows(Exception.class, () -> {
            Config config = new Config();
        });
        Assertions.assertEquals("hdfsuri not set", e.getMessage());
    }

    @Test
    public void configEqualityTest() {
        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            Config config1 = new Config();
            Config config2 = new Config();
            Config config3 = new Config("12345");
            Config config4 = new Config("12345");
            Assertions.assertNotEquals(config1, config2);
            Assertions.assertNotEquals(config1, config3);
            Assertions.assertNotEquals(config3, config4);
        });
    }

    @Test
    public void configConstructorTest() {
        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            Config config1 = new Config();
            Config config2 = new Config("12345");
            Assertions.assertEquals(config1.getHdfsuri(), "hdfs://localhost:45937/");
            Assertions.assertEquals(config2.getHdfsuri(), "12345");
        });
    }

}
