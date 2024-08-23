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
import com.teragrep.cfe_39.consumers.kafka.BatchDistribution;
import com.teragrep.cfe_39.consumers.kafka.KafkaRecordImpl;
import com.teragrep.cfe_39.metrics.DurationStatistics;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

// Tests for processing of consumed kafka records with skipping of broken records enabled (both null and non rfc5424).
public class ProcessingTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingTest.class);

    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static Config config;
    private FileSystem fs;

    // Prepares known state for testing.
    @BeforeEach
    public void startMiniCluster() {
        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            config = new Config();
            // Create a HDFS miniCluster
            baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
            hdfsCluster = new TestMiniClusterFactory().create(config, baseDir);
            fs = new TestFileSystemFactory().create(config.getHdfsuri());
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
    public void normalRecordsTest() {
        // Initialize and register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        assertDoesNotThrow(() -> {

            Consumer<List<KafkaRecordImpl>> output = new BatchDistribution(
                    config, // Configuration settings
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            List<KafkaRecordImpl> recordOffsetObjectList = new ArrayList<>();

            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    0L,
                    "2022-04-25T07:34:50.804Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:50.804Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"835bf792-91cf-44e3-976b-518330bb8fd3\" source=\"source\" unixtime=\"1650872090805\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!"
                            .getBytes(StandardCharsets.UTF_8)
            );
            KafkaRecordImpl recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    1L,
                    "2022-04-25T07:34:50.806Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:50.806Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    2L,
                    "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"1848d8a1-2f08-4a1e-bec4-ff9e6dd92553\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi."
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    3L,
                    "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"5e1a0398-c2a0-468d-a562-c3bb31f0f853\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi."
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    4L,
                    "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"6268c3a2-5bda-427f-acce-29416eb445f4\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi."
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    5L,
                    "2022-04-25T07:34:52.238Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:52.238Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"b500dcaf-1101-4000-b6b9-bfb052ddbf86\" source=\"source\" unixtime=\"1650872092238\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    6L,
                    "2022-04-25T07:34:52.239Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:52.239Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"05363122-51ac-4c0b-a681-f5868081f56d\" source=\"source\" unixtime=\"1650872092239\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    7L,
                    "2022-04-25T07:34:52.239Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:52.239Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"7bbcd843-b795-4c14-b4a1-95f5d445cbcd\" source=\"source\" unixtime=\"1650872092239\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    8L,
                    "2022-04-25T07:34:52.240Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:52.240Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"2bc0a9f9-237d-4656-b40a-3038aace37f0\" source=\"source\" unixtime=\"1650872092240\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    9L,
                    "2022-04-25T07:34:52.240Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:52.240Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"ecf61e8d-e3a7-48ef-9b73-3c5a5243d2e6\" source=\"source\" unixtime=\"1650872092240\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    10L,
                    "2022-04-25T07:34:52.241Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:52.241Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"bf101d5a-e816-4f51-b132-97f8e3431f8e\" source=\"source\" unixtime=\"1650872092241\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    11L,
                    "2022-04-25T07:34:52.241Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:52.241Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"ef94d9e9-3c44-4892-b5a6-bf361d13ff97\" source=\"source\" unixtime=\"1650872092241\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    12L,
                    "2022-04-25T07:34:52.242Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:52.242Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"5bce6e3d-767d-44b4-a044-6c4872f8f2b5\" source=\"source\" unixtime=\"1650872092242\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    13L,
                    "2022-04-25T07:34:52.243Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:52.243Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"3bb55ce4-0ea7-413a-b403-28b174d7ac99\" source=\"source\" unixtime=\"1650872092243\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    14L,
                    "2022-04-25T07:34:52.244Z".getBytes(StandardCharsets.UTF_8),
                    null
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    15L,
                    "2022-04-25T07:34:52.245Z".getBytes(StandardCharsets.UTF_8),
                    "12>1 2022-04-25T07:34:52.245Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"3bb55ce4-0ea7-413a-b403-28b174d7ac99\" source=\"source\" unixtime=\"1650872092243\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            output.accept(recordOffsetObjectList);

            // FIXME: file is not being recognized as avro file for some reason.
        });
    }

    @Test
    public void skipNonRFC5424DatabaseOutputTest() {
        // Initialize and register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        assertDoesNotThrow(() -> {

            Consumer<List<KafkaRecordImpl>> output = new BatchDistribution(
                    config, // Configuration settings
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
            output.accept(recordOffsetObjectList);
            Assertions.assertEquals(1, fs.listStatus(new Path(config.getHdfsPath() + "/" + "topicName")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.1")));
            // File in hdfs does not contain any records, but acts as a marker for kafka consumer offsets.

            // Assert that the file in hdfs contains the expected zero record.

            Path hdfsreadpath = new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.1");
            //Init input stream
            FSDataInputStream inputStream = fs.open(hdfsreadpath);
            //The data is in AVRO-format, so it can't be read as a string.
            DataFileStream<SyslogRecord> reader = new DataFileStream<>(
                    inputStream,
                    new SpecificDatumReader<>(SyslogRecord.class)
            );
            SyslogRecord syslogRecord = null;
            LOGGER.info("\nReading records from file {}:", hdfsreadpath);

            Assertions.assertFalse(reader.hasNext());
        });

    }

    @Test
    public void skipNullRFC5424DatabaseOutputTest() {
        // Initialize and register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        assertDoesNotThrow(() -> {

            Consumer<List<KafkaRecordImpl>> output = new BatchDistribution(
                    config, // Configuration settings
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
            output.accept(recordOffsetObjectList);
            Assertions.assertEquals(1, fs.listStatus(new Path(config.getHdfsPath() + "/" + "topicName")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.1")));
            // File in hdfs does not contain any records, but acts as a marker for kafka consumer offsets.

            // Assert that the file in hdfs contains the expected zero record.

            Path hdfsreadpath = new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.1");
            //Init input stream
            FSDataInputStream inputStream = fs.open(hdfsreadpath);
            //The data is in AVRO-format, so it can't be read as a string.
            DataFileStream<SyslogRecord> reader = new DataFileStream<>(
                    inputStream,
                    new SpecificDatumReader<>(SyslogRecord.class)
            );
            SyslogRecord syslogRecord = null;
            LOGGER.info("\nReading records from file {}:", hdfsreadpath);

            Assertions.assertFalse(reader.hasNext());
        });

    }

    @Test
    public void skipNullAndNonRFC5424DatabaseOutputTest() {
        // Initialize and register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        assertDoesNotThrow(() -> {

            Consumer<List<KafkaRecordImpl>> output = new BatchDistribution(
                    config, // Configuration settings
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            List<KafkaRecordImpl> recordOffsetObjectList = new ArrayList<>();

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
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    2L,
                    "2022-04-25T07:34:50.807Z".getBytes(StandardCharsets.UTF_8),
                    "12>1 2022-04-25T07:34:50.807Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);
            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    3L,
                    "2022-04-25T07:34:50.807Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:50.807Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new KafkaRecordImpl(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);
            output.accept(recordOffsetObjectList);
            Assertions.assertEquals(1, fs.listStatus(new Path(config.getHdfsPath() + "/" + "topicName")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.3")));

            // Assert that the file in hdfs contains the expected single record.

            Path hdfsreadpath = new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.3");
            //Init input stream
            FSDataInputStream inputStream = fs.open(hdfsreadpath);
            //The data is in AVRO-format, so it can't be read as a string.
            DataFileStream<SyslogRecord> reader = new DataFileStream<>(
                    inputStream,
                    new SpecificDatumReader<>(SyslogRecord.class)
            );
            SyslogRecord syslogRecord = null;
            LOGGER.info("\nReading records from file {}:", hdfsreadpath);

            Assertions.assertTrue(reader.hasNext());
            syslogRecord = reader.next(syslogRecord);
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872090807000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"0\", \"offset\": 3, \"origin\": \"jla-02.default\", \"payload\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\"}",
                            syslogRecord.toString()
                    );
            Assertions.assertFalse(reader.hasNext());

        });
    }
}
