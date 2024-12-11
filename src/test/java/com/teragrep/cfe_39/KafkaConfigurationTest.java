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

import com.teragrep.cfe_39.configuration.KafkaConfiguration;
import com.teragrep.cnf_01.PathConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class KafkaConfigurationTest {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaConfigurationTest.class);

    @Test
    public void configurationTest() {
        assertDoesNotThrow(() -> {
            final PathConfiguration kafkaPathConfiguration = new PathConfiguration(
                    System.getProperty("user.dir") + "/src/test/resources/valid.kafka.properties"
            );
            final Map<String, String> kafkaMap;
            kafkaMap = kafkaPathConfiguration.asMap();
            Assertions
                    .assertEquals(
                            "{java.security.auth.login.config=/opt/teragrep/cfe_39/etc/config.jaas, numOfConsumers=2, useMockKafkaConsumer=true, max.poll.records=500, request.timeout.ms=300000, group.id=cfe_39, bootstrap.servers=test, security.protocol=SASL_PLAINTEXT, enable.auto.commit=false, sasl.mechanism=PLAIN, fetch.max.bytes=1073741820, max.poll.interval.ms=300000, auto.offset.reset=earliest}",
                            kafkaMap.toString()
                    );
            KafkaConfiguration kafkaConfig = new KafkaConfiguration(kafkaMap);

            // Assert that printers return correct values.
            Assertions.assertEquals("/opt/teragrep/cfe_39/etc/config.jaas", kafkaConfig.javaSecurityAuthLoginConfig());
            Assertions.assertEquals("test", kafkaConfig.bootstrapServers());
            Assertions.assertEquals("earliest", kafkaConfig.autoOffsetReset());
            Assertions.assertEquals("false", kafkaConfig.enableAutoCommit());
            Assertions.assertEquals("cfe_39", kafkaConfig.groupId());
            Assertions.assertEquals("SASL_PLAINTEXT", kafkaConfig.securityProtocol());
            Assertions.assertEquals("PLAIN", kafkaConfig.saslMechanism());
            Assertions.assertEquals(500, kafkaConfig.maxPollRecords());
            Assertions.assertEquals(1073741820, kafkaConfig.fetchMaxBytes());
            Assertions.assertEquals(300000, kafkaConfig.requestTimeoutMs());
            Assertions.assertEquals(300000, kafkaConfig.maxPollIntervalMs());
            Assertions.assertTrue(kafkaConfig.useMockKafkaConsumer());
            Assertions.assertEquals(2, kafkaConfig.numOfConsumers());

        });
    }
}
