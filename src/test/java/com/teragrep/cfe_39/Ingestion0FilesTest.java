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
import com.teragrep.cfe_39.configuration.CommonConfiguration;
import com.teragrep.cfe_39.configuration.HdfsConfiguration;
import com.teragrep.cfe_39.configuration.KafkaConfiguration;
import com.teragrep.cfe_39.consumers.kafka.HdfsDataIngestion;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.nio.file.Files;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class Ingestion0FilesTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ingestion0FilesTest.class);
    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static CommonConfiguration config;
    private static HdfsConfiguration hdfsConfig;
    private static KafkaConfiguration kafkaConfig;
    private FileSystem fs;

    // Prepares known state for testing.
    @BeforeEach
    public void startMiniCluster() {
        assertDoesNotThrow(() -> {
            File queueDir = new File(System.getProperty("user.dir") + "/target/AVRO");
            if (!queueDir.exists()) {
                queueDir.mkdirs();
            }
            Map<String, String> map = new HashMap<>();
            map.put("log4j2.configurationFile", "/opt/teragrep/cfe_39/etc/log4j2.properties");
            map.put("egress.configurationFile", "/opt/teragrep/cfe_39/etc/egress.properties");
            map.put("ingress.configurationFile", "/opt/teragrep/cfe_39/etc/ingress.properties");
            map.put("queueDirectory", System.getProperty("user.dir") + "/target/AVRO/");
            map.put("queueTopicPattern", "^testConsumerTopic-*$");
            map.put("skipNonRFC5424Records", "true");
            map.put("skipEmptyRFC5424Records", "true");
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
            hdfsMap.put("maximumFileSize", "30000");
            hdfsConfig = new HdfsConfiguration(hdfsMap);
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
            kafkaMap.put("numOfConsumers", "2");
            kafkaConfig = new KafkaConfiguration(kafkaMap);
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

    @DisabledIfSystemProperty(
            named = "skipIngestionTest",
            matches = "true"
    )
    @Test
    public void ingestion0FilesTest() {
        /*This test case is for testing the functionality of the ingestion when there are no files already present in the database before starting ingestion.
        Maximum file size is set to 30,000 in the config.
        Empty HDFS database, 160 records in mock kafka consumer ready for ingestion. All 14 records for each 10 topic partitions are stored in a single avro-file per partition (2 skipped records per file).*/
        assertDoesNotThrow(() -> {
            Assertions.assertTrue(hdfsConfig.pruneOffset() >= 300000L); // Fails the test if the config is not correct.
            Assertions.assertFalse(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic")));
            HdfsDataIngestion hdfsDataIngestion = new HdfsDataIngestion(config, hdfsConfig, kafkaConfig);
            hdfsDataIngestion.run();
        });

        // Assert that the kafka records were ingested correctly and the database/temporary file holds the correct 140 records (20 broken records were skipped).
        assertDoesNotThrow(() -> {
            String path = hdfsConfig.hdfsPath() + "/" + "testConsumerTopic";
            Path newDirectoryPath = new Path(path);
            Assertions.assertTrue(fs.exists(newDirectoryPath));

            /* This is the HDFS write path for the files:
             Path hdfswritepath = new Path(newDirectoryPath + "/" + fileName); where newDirectoryPath is config.getHdfsPath() + "/" + lastObject.topic; and filename is lastObject.partition+"."+lastObject.offset;.*/
            List<String> filenameList = new ArrayList<>();
            for (int i = 0; i <= 9; i++) {
                filenameList.add("testConsumerTopic" + i + "." + 1);
            }
            FileStatus[] fileStatuses = fs.listStatus(newDirectoryPath);
            Assertions.assertEquals(0, fileStatuses.length);
            LOGGER.debug("No files present in HDFS as expected as maximum file size hasn't been reached.");

            // Assert that all the records are inside the temporary AVRO-files generated by PartitionFile objects during consumption.

            File queueDirectory = new File(config.queueDirectory());
            File[] files = queueDirectory.listFiles();
            Assertions.assertEquals(10, files.length);
            for (File file : files) {
                Assertions.assertTrue(filenameList.contains(file.getName()));
            }

            int partitionCounter = 0;
            for (String fileName : filenameList) {

                String path2 = config.queueDirectory() + "/" + fileName;
                File avroFile = new File(path2);

                Assertions.assertTrue(filenameList.contains(avroFile.getName()));
                DatumReader<SyslogRecord> datumReader = new SpecificDatumReader<>(SyslogRecord.class);
                DataFileReader<SyslogRecord> reader = new DataFileReader<>(avroFile, datumReader);

                Assertions.assertTrue(reader.hasNext());
                SyslogRecord record = reader.next();
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872090804000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 0, \"origin\": \"jla-02.default\", \"payload\": \"[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872090806000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 1, \"origin\": \"jla-02.default\", \"payload\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 2, \"origin\": \"jla-02\", \"payload\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 3, \"origin\": \"jla-02\", \"payload\": \"470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 4, \"origin\": \"jla-02\", \"payload\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092238000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 5, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092239000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 6, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092239000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 7, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092240000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 8, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092240000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 9, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092241000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 10, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092241000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 11, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092242000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 12, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092243000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 13, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]\"}",
                                record.toString()
                        );
                Assertions.assertFalse(reader.hasNext());
                LOGGER.info("Partition {} passed assertions.", partitionCounter);
                partitionCounter++;
                reader.close();
                avroFile.delete();
            }
            Assertions.assertEquals(10, partitionCounter);
        });
    }
}
