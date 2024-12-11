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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class Ingestion1Old1NewFileTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ingestion1Old1NewFileTest.class);
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

            // Inserts pre-made avro-files to HDFS where one file has new timestamp and other old, which are normally generated during data ingestion from mock kafka consumer.
            String path = hdfsConfig.hdfsPath() + "/" + "testConsumerTopic"; // "hdfs:///opt/teragrep/cfe_39/srv/testConsumerTopic"
            // Sets the directory where the data should be stored, if the directory doesn't exist then it's created.
            Path newDirectoryPath = new Path(path);
            // Create new Directory
            fs.mkdirs(newDirectoryPath);
            LOGGER.debug("Path {} created.", path);
            String dir = System.getProperty("user.dir") + "/src/test/resources/mockHdfsFiles";
            Set<String> listOfFiles = Stream
                    .of(Objects.requireNonNull(new File(dir).listFiles()))
                    .filter(file -> !file.isDirectory())
                    .map(File::getName)
                    .collect(Collectors.toSet());
            // Loop through all the avro files
            for (String fileName : listOfFiles) {
                String pathname = dir + "/" + fileName;
                File avroFile = new File(pathname);
                //==== Write file
                LOGGER.debug("Begin Write file into hdfs");
                //Create a path
                Path hdfswritepath = new Path(newDirectoryPath + "/" + avroFile.getName()); // filename should be set according to the requirements: 0.12345 where 0 is Kafka partition and 12345 is Kafka offset.
                Assertions.assertFalse(fs.exists(hdfswritepath));
                Path readPath = new Path(avroFile.getPath());
                fs.copyFromLocalFile(readPath, hdfswritepath);
                LOGGER.debug("End Write file into hdfs");
                LOGGER.debug("\nFile committed to HDFS, file writepath should be: {}\n", hdfswritepath);
            }
            fs.setTimes(new Path("hdfs:/opt/teragrep/cfe_39/srv/testConsumerTopic/0.9"), 157784760000L, -1);
            fs.setTimes(new Path("hdfs:/opt/teragrep/cfe_39/srv/testConsumerTopic/0.13"), -1, -1);
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
    public void ingestion1Old1NewFileTest() {
        /* This test case is for testing the functionality of the ingestion when there are files already present in the database before starting ingestion.
         14 records are inserted to HDFS database before starting ingestion, with 126/160 records in mock kafka consumer ready for ingestion (20 broken records + 14 records already in HDFS).
         Partitions through 1 to 9 will have a single local file each with each containing 14 records. Partition 0 will have 3 files, 0.9 and 0.13 in HDFS and one empty local file.
         partition 0 HDFS files are pre-made and inserted to the HDFS database with old timestamp for file 0.9 and new for 0.13.
         Old files are pruned from the database during ingestion topic scan loops.*/

        assertDoesNotThrow(() -> {
            // Assert the known starting state.
            Assertions.assertTrue(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic")));
            Assertions
                    .assertEquals(2, fs.listStatus(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
            Assertions
                    .assertTrue(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic" + "/" + "0.13")));
            Assertions.assertTrue(hdfsConfig.pruneOffset() >= 300000L); // Fails the test if the config is not correct.
            Assertions.assertTrue((System.currentTimeMillis() - hdfsConfig.pruneOffset()) > 157784760000L);
            HdfsDataIngestion hdfsDataIngestion = new HdfsDataIngestion(config, hdfsConfig, kafkaConfig);
            hdfsDataIngestion.run();

            // Assert that the kafka records were ingested and pruned correctly and the database holds only the expected 1 file.
            Assertions
                    .assertEquals(1, fs.listStatus(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions
                    .assertFalse(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
            Assertions
                    .assertTrue(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic" + "/" + "0.13")));

            // Assert the avro-files that were too small to be stored in HDFS.
            String path1 = config.queueDirectory() + "/" + "testConsumerTopic0.1";
            File avroFile1 = new File(path1);
            Assertions.assertFalse(avroFile1.exists()); // Partition 0 avro-file shouldn't exist because there are no records left in the buffer.

            List<String> filenameList = new ArrayList<>();
            for (int i = 1; i <= 9; i++) {
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
