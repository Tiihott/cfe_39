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
import com.teragrep.cfe_39.consumers.kafka.KafkaRecordImpl;
import com.teragrep.cfe_39.consumers.kafka.SyslogAvroWriter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class SyslogAvroWriterTest {

    private static Config config;

    // Prepares known state for testing.
    @BeforeEach
    public void startMiniCluster() {
        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            config = new Config();
        });
    }

    // Teardown the minicluster
    @AfterEach
    public void teardownMiniCluster() {
        File queueDirectory = new File(config.getQueueDirectory());
        File[] files = queueDirectory.listFiles();
        if (files[0].getName().equals("topicName0.1")) {
            files[0].delete();
        }
    }

    @Test
    public void writeTest() {

        assertDoesNotThrow(() -> {

            File queueDirectory = new File(config.getQueueDirectory());

            File syslogFile = new File(config.getQueueDirectory() + File.separator + "topicName0.1");

            ConsumerRecord<byte[], byte[]> record0 = new ConsumerRecord<>(
                    "topicName",
                    0,
                    0L,
                    "2022-04-25T07:34:50.804Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:50.804Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"835bf792-91cf-44e3-976b-518330bb8fd3\" source=\"source\" unixtime=\"1650872090805\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!"
                            .getBytes(StandardCharsets.UTF_8)
            );
            KafkaRecordImpl recordOffsetObject0 = new KafkaRecordImpl(
                    record0.topic(),
                    record0.partition(),
                    record0.offset(),
                    record0.value()
            );

            ConsumerRecord<byte[], byte[]> record1 = new ConsumerRecord<>(
                    "topicName",
                    0,
                    1L,
                    "2022-04-25T07:34:50.806Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:50.806Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!"
                            .getBytes(StandardCharsets.UTF_8)
            );
            KafkaRecordImpl recordOffsetObject1 = new KafkaRecordImpl(
                    record1.topic(),
                    record1.partition(),
                    record1.offset(),
                    record1.value()
            );

            ConsumerRecord<byte[], byte[]> record2 = new ConsumerRecord<>(
                    "topicName",
                    0,
                    2L,
                    "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"1848d8a1-2f08-4a1e-bec4-ff9e6dd92553\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi."
                            .getBytes(StandardCharsets.UTF_8)
            );
            KafkaRecordImpl recordOffsetObject2 = new KafkaRecordImpl(
                    record2.topic(),
                    record2.partition(),
                    record2.offset(),
                    record2.value()
            );

            try (SyslogAvroWriter syslogAvroWriter = new SyslogAvroWriter(syslogFile)) {
                syslogAvroWriter.write(recordOffsetObject0.toSyslogRecord());
                syslogAvroWriter.write(recordOffsetObject1.toSyslogRecord());
                syslogAvroWriter.write(recordOffsetObject2.toSyslogRecord());
            }
            try (SyslogAvroWriter syslogAvroWriter = new SyslogAvroWriter(syslogFile)) {
                syslogAvroWriter.write(recordOffsetObject2.toSyslogRecord());
            }
            DatumReader<SyslogRecord> datumReader = new SpecificDatumReader<>(SyslogRecord.class);
            DataFileReader<SyslogRecord> dataFileReader = new DataFileReader<>(syslogFile, datumReader);
            Assertions.assertTrue(dataFileReader.hasNext());
            SyslogRecord next = dataFileReader.next();
            Assertions.assertEquals(0, next.getOffset());
            Assertions.assertTrue(dataFileReader.hasNext());
            next = dataFileReader.next();

            Assertions.assertEquals(1, next.getOffset());
            Assertions.assertTrue(dataFileReader.hasNext());
            next = dataFileReader.next();

            Assertions.assertEquals(2, next.getOffset());
            Assertions.assertTrue(dataFileReader.hasNext());
            next = dataFileReader.next();

            Assertions.assertEquals(2, next.getOffset());
            dataFileReader.close();

        });

    }
}
