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
import com.teragrep.cfe_39.configuration.ConfigurationImpl;
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
    private static ConfigurationImpl config;
    private FileSystem fs;

    // Prepares known state for testing.
    @BeforeEach
    public void startMiniCluster() {
        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration with skipping of broken records disabled.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            config = new ConfigurationImpl().loadPropertiesFile();
            // Create a HDFS miniCluster
            baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
            hdfsCluster = new TestMiniClusterFactory().create(config, baseDir);
            config = config.with("hdfsuri", "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/");
            config = config.with("maximumFileSize", "30000");
            config = config.with("queueDirectory", System.getProperty("user.dir") + "/etc/AVRO/");
            config = config
                    .with("log4j2.configurationFile", System.getProperty("user.dir") + "/rpm/resources/log4j2.properties");
            config.configureLogging();
            fs = new TestFileSystemFactory().create(config.valueOf("hdfsuri"));
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
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );
            BatchDistributionImpl output2 = new BatchDistributionImpl(
                    config, // Configuration settings
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            ReadCoordinator readCoordinator = new ReadCoordinator(
                    "testConsumerTopic",
                    config,
                    output1,
                    hdfsStartOffsets
            );
            Thread readThread = new Thread(null, readCoordinator, "testConsumerTopic1"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(1000);

            ReadCoordinator readCoordinator2 = new ReadCoordinator(
                    "testConsumerTopic",
                    config,
                    output2,
                    hdfsStartOffsets
            );
            Thread readThread2 = new Thread(null, readCoordinator2, "testConsumerTopic2"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread2.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(10000);

            // Because BatchDistributionImpl can not be used as a functional interface, must do assertion through avro-files until better solution is found (add fake to interface?).

            // Assert the records inside the avro-files
            List<String> filenameList = new ArrayList<>();
            for (int i = 0; i <= 9; i++) {
                filenameList.add("testConsumerTopic" + i + "." + 1);
            }
            for (String fileName : filenameList) {
                String path2 = config.valueOf("queueDirectory") + "/" + fileName;
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
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            ReadCoordinator readCoordinator = new ReadCoordinator(
                    "testConsumerTopic",
                    config,
                    output,
                    hdfsStartOffsets
            );
            Thread readThread = new Thread(null, readCoordinator, "testConsumerTopic0"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(10000);

            // Because BatchDistributionImpl can not be used as a functional interface, must do assertion through avro-files until better solution is found (add fake to interface?).

            // Assert the records inside the avro-files
            List<String> filenameList = new ArrayList<>();
            for (int i = 0; i <= 9; i++) {
                filenameList.add("testConsumerTopic" + i + "." + 1);
            }
            for (String fileName : filenameList) {
                String path2 = config.valueOf("queueDirectory") + "/" + fileName;
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
