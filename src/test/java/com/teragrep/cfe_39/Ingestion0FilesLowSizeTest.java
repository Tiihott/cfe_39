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
import com.teragrep.cfe_39.consumers.kafka.HdfsDataIngestion;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class Ingestion0FilesLowSizeTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ingestion0FilesTest.class);
    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static ConfigurationImpl config;
    private FileSystem fs;

    // Prepares known state for testing.
    @BeforeEach
    public void startMiniCluster() {
        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            config = new ConfigurationImpl();
            config
                    .loadPropertiesFile(
                            System
                                    .getProperty(
                                            "cfe_39.config.location", "/opt/teragrep/cfe_39/etc/application.properties"
                                    )
                    );
            // Create a HDFS miniCluster
            baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
            hdfsCluster = new TestMiniClusterFactory().create(config, baseDir);
            config.with("hdfsuri", "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/");
            config.with("maximumFileSize", "3000");
            config.with("queueDirectory", System.getProperty("user.dir") + "/etc/AVRO/");
            config.with("hadoop.security.authentication", "false");
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

    @DisabledIfSystemProperty(
            named = "skipIngestionTest",
            matches = "true"
    )
    @Test
    public void ingestion0FilesLowSizeTest() {
        /*This test case is for testing the functionality of the ingestion when there are files already present in the database before starting ingestion.
        Maximum file size is set to 3,000 in the config.
        Empty HDFS database, 140 records in mock kafka consumer ready for ingestion. All 14 records for each 10 topic partitions are stored in two avro-files per partition based on MaximumFileSize.*/
        assertDoesNotThrow(() -> {
            Assertions.assertTrue(Long.parseLong(config.valueOf("pruneOffset")) >= 300000L); // Fails the test if the config is not correct.
            Assertions.assertFalse(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic")));
            HdfsDataIngestion hdfsDataIngestion = new HdfsDataIngestion(config);
            hdfsDataIngestion.run();
        });

        // Assert that the kafka records were ingested correctly and the database holds the correct 140 records.

        // Check that the files were properly written to HDFS.
        String hdfsuri = config.valueOf("hdfsuri");

        String path = config.valueOf("hdfsPath") + "/" + "testConsumerTopic";
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        //Get the filesystem - HDFS
        assertDoesNotThrow(() -> {
            fs = FileSystem.get(URI.create(hdfsuri), conf);

            Path workingDir = fs.getWorkingDirectory();
            Path newDirectoryPath = new Path(path);
            Assertions.assertTrue(fs.exists(newDirectoryPath));

            // Assert that the kafka records were ingested correctly and the database holds the expected 20 files.
            FileStatus[] fileStatuses = fs.listStatus(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic"));

            Assertions
                    .assertEquals(10, fs.listStatus(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic")).length);
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "0.10")));
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "1.10")));
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "2.10")));
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "3.10")));
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "4.10")));
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "5.10")));
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "6.10")));
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "7.10")));
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "8.10")));
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "9.10")));
            LOGGER.debug("All expected files present in HDFS.");

            // Now Assert the files that were too small to be stored in HDFS.

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

                for (int i = 11; i <= 13; i++) {
                    Assertions.assertTrue(reader.hasNext());
                    SyslogRecord record = reader.next();
                    Assertions.assertEquals(i, record.getOffset());
                }
                Assertions.assertFalse(reader.hasNext());
                reader.close();
                avroFile.delete();
                ;
            }
        });
    }
}
