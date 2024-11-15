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

import com.teragrep.cfe_39.avro.SyslogRecord;
import com.teragrep.cfe_39.configuration.NewCommonConfiguration;
import com.teragrep.cfe_39.configuration.NewHdfsConfiguration;
import com.teragrep.cfe_39.configuration.NewKafkaConfiguration;
import com.teragrep.cfe_39.consumers.kafka.BatchDistributionImpl;
import com.teragrep.cfe_39.consumers.kafka.ReadCoordinator;
import com.teragrep.cfe_39.metrics.DurationStatistics;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class KafkaConsumerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerTest.class);

    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static NewCommonConfiguration config;
    private static NewHdfsConfiguration hdfsConfig;
    private static NewKafkaConfiguration kafkaConfig;
    private FileSystem fs;

    // Prepares known state for testing.
    @BeforeEach
    public void startMiniCluster() {
        assertDoesNotThrow(() -> {
            Map<String, String> map = new HashMap<>();
            map.put("log4j2.configurationFile", "/opt/teragrep/cfe_39/etc/log4j2.properties");
            map.put("egress.configurationFile", "/opt/teragrep/cfe_39/etc/egress.properties");
            map.put("ingress.configurationFile", "/opt/teragrep/cfe_39/etc/ingress.properties");
            map.put("queueDirectory", System.getProperty("user.dir") + "/etc/AVRO/");
            map.put("maximumFileSize", "30000");
            map.put("queueTopicPattern", "^testConsumerTopic-*$");
            map.put("numOfConsumers", "2");
            map.put("skipNonRFC5424Records", "true");
            map.put("skipEmptyRFC5424Records", "true");
            map.put("pruneOffset", "157784760000");
            map.put("consumerTimeout", "600000");
            config = new NewCommonConfiguration(map);

            // Create a HDFS miniCluster
            baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
            hdfsCluster = new TestMiniClusterFactory().create(baseDir);
            Map<String, String> hdfsMap = new HashMap<>();
            hdfsMap.put("pruneOffset", "157784760000");
            hdfsMap.put("hdfsuri", "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/");
            hdfsMap.put("hdfsPath", "hdfs:///opt/teragrep/cfe_39/srv/");
            hdfsMap.put("java.security.krb5.kdc", "test");
            hdfsMap.put("java.security.krb5.realm", "test");
            hdfsMap.put("hadoop.security.authentication", "false");
            hdfsMap.put("hadoop.security.authorization", "test");
            hdfsMap.put("dfs.namenode.kerberos.principal.pattern", "test");
            hdfsMap.put("KerberosKeytabUser", "test");
            hdfsMap.put("KerberosKeytabPath", "test");
            hdfsMap.put("dfs.client.use.datanode.hostname", "false");
            hdfsMap.put("hadoop.kerberos.keytab.login.autorenewal.enabled", "true");
            hdfsMap.put("dfs.data.transfer.protection", "test");
            hdfsMap.put("dfs.encrypt.data.transfer.cipher.suites", "test");
            hdfsConfig = new NewHdfsConfiguration(hdfsMap);
            fs = new TestFileSystemFactory().create(hdfsConfig.hdfsUri());

            Map<String, String> kafkaMap = new HashMap<>();
            kafkaMap.put("java.security.auth.login.config", "/opt/teragrep/cfe_39/etc/config.jaas");
            kafkaMap.put("bootstrap.servers", "test");
            kafkaMap.put("auto.offset.reset", "earliest");
            kafkaMap.put("enable.auto.commit", "false");
            kafkaMap.put("group.id", "cfe_39");
            kafkaMap.put("security.protocol", "SASL_PLAINTEXT");
            kafkaMap.put("sasl.mechanism", "PLAIN");
            kafkaMap.put("max.poll.records", "500");
            kafkaMap.put("fetch.max.bytes", "1073741820");
            kafkaMap.put("request.timeout.ms", "300000");
            kafkaMap.put("max.poll.interval.ms", "300000");
            kafkaMap.put("useMockKafkaConsumer", "true");
            kafkaConfig = new NewKafkaConfiguration(kafkaMap);
        });
    }

    // Teardown the minicluster
    @AfterEach
    public void teardownMiniCluster() {
        assertDoesNotThrow(() -> {
            fs.close();
        });
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }

    @Test
    public void readCoordinatorTest2Threads() {
        assertDoesNotThrow(() -> {
            Map<TopicPartition, Long> hdfsStartOffsets = new HashMap<>();
            DurationStatistics durationStatistics = new DurationStatistics();
            durationStatistics.register();
            // BatchDistributionImpl can not be used as a functional interface.
            BatchDistributionImpl output1 = new BatchDistributionImpl(
                    config, // Configuration settings
                    hdfsConfig,
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );
            BatchDistributionImpl output2 = new BatchDistributionImpl(
                    config, // Configuration settings
                    hdfsConfig,
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            ReadCoordinator readCoordinator = new ReadCoordinator(
                    "testConsumerTopic",
                    config,
                    kafkaConfig,
                    hdfsConfig,
                    output1,
                    hdfsStartOffsets
            );
            Thread readThread = new Thread(null, readCoordinator, "testConsumerTopic1"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            ReadCoordinator readCoordinator2 = new ReadCoordinator(
                    "testConsumerTopic",
                    config,
                    kafkaConfig,
                    hdfsConfig,
                    output2,
                    hdfsStartOffsets
            );
            Thread readThread2 = new Thread(null, readCoordinator2, "testConsumerTopic2"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread2.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(10000); // Allow read threads to have enough time to execute their tasks properly.

            // Because BatchDistributionImpl can not be used as a functional interface, must do assertion through avro-files until better solution is found (add fake to interface?).

            // Assert the records inside the avro-files
            List<String> filenameList = new ArrayList<>();
            for (int i = 0; i <= 9; i++) {
                filenameList.add("testConsumerTopic" + i + "." + 1);
            }
            for (String fileName : filenameList) {
                String path2 = config.queueDirectory() + "/" + fileName;
                File avroFile = new File(path2);
                Assertions.assertTrue(filenameList.contains(avroFile.getName()));
                DatumReader<SyslogRecord> datumReader = new SpecificDatumReader<>(SyslogRecord.class);
                DataFileReader<SyslogRecord> reader = new DataFileReader<>(avroFile, datumReader);
                for (int i = 0; i <= 13; i++) {
                    Assertions.assertTrue(reader.hasNext());
                    SyslogRecord record = reader.next();
                    Assertions.assertEquals(i, record.getOffset());
                }
                Assertions.assertFalse(reader.hasNext());
                reader.close();
                avroFile.delete();
            }

        });
    }

    @Test
    public void readCoordinatorTest1Thread() {

        assertDoesNotThrow(() -> {
            Map<TopicPartition, Long> hdfsStartOffsets = new HashMap<>();
            DurationStatistics durationStatistics = new DurationStatistics();
            durationStatistics.register();
            // BatchDistributionImpl can not be used as a functional interface.
            BatchDistributionImpl output = new BatchDistributionImpl(
                    config, // Configuration settings
                    hdfsConfig,
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            ReadCoordinator readCoordinator = new ReadCoordinator(
                    "testConsumerTopic",
                    config,
                    kafkaConfig,
                    hdfsConfig,
                    output,
                    hdfsStartOffsets
            );
            Thread readThread = new Thread(null, readCoordinator, "testConsumerTopic0"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(10000); // Allow read thread to have enough time to execute the task properly.

            // Because BatchDistributionImpl can not be used as a functional interface, must do assertion through avro-files until better solution is found (add fake to interface?).

            // Assert the records inside the avro-files
            List<String> filenameList = new ArrayList<>();
            for (int i = 0; i <= 9; i++) {
                filenameList.add("testConsumerTopic" + i + "." + 1);
            }
            for (String fileName : filenameList) {
                String path2 = config.queueDirectory() + "/" + fileName;
                File avroFile = new File(path2);
                Assertions.assertTrue(filenameList.contains(avroFile.getName()));
                DatumReader<SyslogRecord> datumReader = new SpecificDatumReader<>(SyslogRecord.class);
                DataFileReader<SyslogRecord> reader = new DataFileReader<>(avroFile, datumReader);
                for (int i = 0; i <= 13; i++) {
                    Assertions.assertTrue(reader.hasNext());
                    SyslogRecord record = reader.next();
                    Assertions.assertEquals(i, record.getOffset());
                }
                Assertions.assertFalse(reader.hasNext());
                reader.close();
                avroFile.delete();
            }

        });
    }

}
