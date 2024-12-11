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

import com.teragrep.cfe_39.configuration.CommonConfiguration;
import com.teragrep.cfe_39.configuration.HdfsConfiguration;
import com.teragrep.cfe_39.consumers.kafka.BatchDistributionImpl;
import com.teragrep.cfe_39.consumers.kafka.KafkaRecordImpl;
import com.teragrep.cfe_39.metrics.DurationStatistics;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

// Tests for processing of consumed kafka records with skipping of broken records disabled (both null and non rfc5424).
public class ProcessingFailureTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingFailureTest.class);

    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static CommonConfiguration config;
    private static HdfsConfiguration hdfsConfig;
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
            map.put("queueTopicPattern", "^testConsumerTopic-*$");
            map.put("skipNonRFC5424Records", "false");
            map.put("skipEmptyRFC5424Records", "false");
            map.put("pruneOffset", "157784760000");
            map.put("consumerTimeout", "600000");
            config = new CommonConfiguration(map);

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
            hdfsMap.put("maximumFileSize", "3000");
            hdfsConfig = new HdfsConfiguration(hdfsMap);
            fs = new TestFileSystemFactory().create(hdfsConfig.hdfsUri());
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
    public void failNonRFC5424DatabaseOutputTest() {
        // Initialize and register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        assertDoesNotThrow(() -> {

            BatchDistributionImpl output = new BatchDistributionImpl(
                    config, // Configuration settings
                    hdfsConfig,
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    1L,
                    "2022-04-25T07:34:50.806Z".getBytes(StandardCharsets.UTF_8),
                    "12>1 2022-04-25T07:34:50.806Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!"
                            .getBytes(StandardCharsets.UTF_8)
            );
            KafkaRecordImpl recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );

            List<KafkaRecordImpl> recordOffsetObjectList = new ArrayList<>();
            recordOffsetObjectList.add(recordOffsetObject);
            Exception e = Assertions.assertThrows(Exception.class, () -> output.accept(recordOffsetObjectList));
            Assertions.assertEquals("com.teragrep.rlo_06.PriorityParseException: PRIORITY < missing", e.getMessage());
            Assertions.assertFalse(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "topicName" + "/" + "0.1")));
            // No files stored to hdfs.

            // Assert the local avro file that should e empty.
            File queueDirectory = new File(config.queueDirectory());
            File[] files = queueDirectory.listFiles();
            Assertions.assertEquals(0, files.length); // Partition 0 avro-file shouldn't exist because there are no records left in the buffer.
        });

    }

    @Test
    public void failNullRFC5424DatabaseOutputTest() {
        // Initialize and register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        assertDoesNotThrow(() -> {

            BatchDistributionImpl output = new BatchDistributionImpl(
                    config, // Configuration settings
                    hdfsConfig,
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    1L,
                    "2022-04-25T07:34:50.806Z".getBytes(StandardCharsets.UTF_8),
                    null
            );
            KafkaRecordImpl recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );

            List<KafkaRecordImpl> recordOffsetObjectList = new ArrayList<>();
            recordOffsetObjectList.add(recordOffsetObject);
            RuntimeException e = Assertions
                    .assertThrows(RuntimeException.class, () -> output.accept(recordOffsetObjectList));
            Assertions
                    .assertEquals(
                            "java.lang.NullPointerException: Cannot read the array length because \"buf\" is null",
                            e.getMessage()
                    );
            Assertions.assertFalse(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "topicName" + "/" + "0.1")));
            // No files stored to hdfs.

            // Assert the local avro file that should be empty.
            File queueDirectory = new File(config.queueDirectory());
            File[] files = queueDirectory.listFiles();
            Assertions.assertEquals(0, files.length); // Partition 0 avro-file shouldn't exist because there are no records left in the buffer.
        });

    }
}
