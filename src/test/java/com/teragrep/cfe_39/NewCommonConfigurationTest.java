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
import com.teragrep.cnf_01.PathConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class NewCommonConfigurationTest {

    private final Logger LOGGER = LoggerFactory.getLogger(NewCommonConfigurationTest.class);

    @Test
    public void configurationTest() {
        assertDoesNotThrow(() -> {
            final PathConfiguration pathConfiguration = new PathConfiguration(
                    System.getProperty("user.dir") + "/src/test/resources/valid.application.properties"
            );
            final Map<String, String> map;
            map = pathConfiguration.asMap();
            Assertions
                    .assertEquals(
                            "{pruneOffset=157784760000, numOfConsumers=2, queueTopicPattern=^testConsumerTopic-*$, queueDirectory=/opt/teragrep/cfe_39/etc/AVRO/, skipNonRFC5424Records=true, maximumFileSize=3000, skipEmptyRFC5424Records=true, consumerTimeout=600000}",
                            map.toString()
                    );
            NewCommonConfiguration commonConfig = new NewCommonConfiguration(map);

            // Assert that printers return correct values.
            Assertions
                    .assertEquals(System.getProperty("user.dir") + "/rpm/resources/egress.properties", commonConfig.egressConfigurationFile());
            Assertions
                    .assertEquals(System.getProperty("user.dir") + "/rpm/resources/ingress.properties", commonConfig.ingressConfigurationFile());
            Assertions
                    .assertEquals(System.getProperty("user.dir") + "/rpm/resources/log4j2.properties", commonConfig.log4j2ConfigurationFile());
            Assertions.assertEquals(2, commonConfig.numOfConsumers());
            Assertions.assertEquals(3000, commonConfig.maximumFileSize());
            Assertions.assertEquals(600000, commonConfig.consumerTimeout());
            Assertions.assertEquals(157784760000L, commonConfig.pruneOffset());
            Assertions.assertTrue(commonConfig.skipNonRFC5424Records());
            Assertions.assertTrue(commonConfig.skipEmptyRFC5424Records());
            Assertions.assertEquals("/opt/teragrep/cfe_39/etc/AVRO/", commonConfig.queueDirectory());
            Assertions.assertEquals("^testConsumerTopic-*$", commonConfig.queueTopicPattern());
        });
    }
}
