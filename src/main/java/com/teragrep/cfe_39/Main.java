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

import com.teragrep.cfe_39.configuration.NewCommonConfiguration;
import com.teragrep.cfe_39.configuration.NewHdfsConfiguration;
import com.teragrep.cfe_39.configuration.NewKafkaConfiguration;
import com.teragrep.cfe_39.consumers.kafka.HdfsDataIngestion;
import com.teragrep.cnf_01.ConfigurationException;
import com.teragrep.cnf_01.PathConfiguration;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public final class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        // NewCommonConfiguration
        final PathConfiguration pathConfiguration = new PathConfiguration(
                System.getProperty("cfe_39.config.location", "/opt/teragrep/cfe_39/etc/application.properties")
        );
        final Map<String, String> map;
        try {
            map = pathConfiguration.asMap();
        }
        catch (ConfigurationException e) {
            LOGGER.error("Failed to create PathConfiguration: <{}>", e.getMessage());
            throw e;
        }
        NewCommonConfiguration commonConfig = new NewCommonConfiguration(map);

        // log4j2 configuration
        Path log4j2Config = Paths
                .get(commonConfig.log4j2ConfigurationFile(), System.getProperty("user.dir") + "/rpm/resources/log4j2.properties");
        Configurator.reconfigure(log4j2Config.toUri());

        // NewKafkaConfiguration
        final PathConfiguration kafkaPathConfiguration = new PathConfiguration(commonConfig.egressConfigurationFile());
        final Map<String, String> kafkaMap;
        try {
            kafkaMap = kafkaPathConfiguration.asMap();
        }
        catch (ConfigurationException e) {
            LOGGER.error("Failed to create PathConfiguration: <{}>", e.getMessage());
            throw e;
        }
        NewKafkaConfiguration kafkaConfig = new NewKafkaConfiguration(kafkaMap);

        // NewHdfsConfiguration
        final PathConfiguration hdfsPathConfiguration = new PathConfiguration(commonConfig.ingressConfigurationFile());
        final Map<String, String> hdfsMap;
        try {
            hdfsMap = hdfsPathConfiguration.asMap();
        }
        catch (ConfigurationException e) {
            LOGGER.error("Failed to create PathConfiguration: <{}>", e.getMessage());
            throw e;
        }
        NewHdfsConfiguration hdfsConfig = new NewHdfsConfiguration(hdfsMap);

        LOGGER.info("Running main program");
        HdfsDataIngestion hdfsDataIngestion = new HdfsDataIngestion(commonConfig, hdfsConfig, kafkaConfig);
        hdfsDataIngestion.run();
    }
}
