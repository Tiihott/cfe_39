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

import com.teragrep.cfe_39.consumers.kafka.HdfsDataIngestion;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class Ingestion1Old1NewFileTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ingestion1Old1NewFileTest.class);
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
            config = new Config("hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/");
            fs = new TestFileSystemFactory().create(config.getHdfsuri());

            // Inserts pre-made avro-files to HDFS where one file has new timestamp and other old, which are normally generated during data ingestion from mock kafka consumer.
            String path = config.getHdfsPath() + "/" + "testConsumerTopic"; // "hdfs:///opt/teragrep/cfe_39/srv/testConsumerTopic"
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
         14 records are inserted to HDFS database before starting ingestion, with 124/140 records in mock kafka consumer ready for ingestion.
         Partitions through 1 to 9 will have only a single file, partition 0 will have 2 files (0.9 and 0.13).
         partition 0 files are pre-made and inserted to the HDFS database with old timestamp for file 0.9 and new for 0.13.
         Old files are pruned from the database during ingestion topic scan loops.*/

        assertDoesNotThrow(() -> {
            // Assert the known starting state.
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")));
            Assertions
                    .assertEquals(2, fs.listStatus(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.13")));
            Assertions.assertTrue(config.getPruneOffset() >= 300000L); // Fails the test if the config is not correct.
            Assertions.assertTrue((System.currentTimeMillis() - config.getPruneOffset()) > 157784760000L);
            config.setMaximumFileSize(30000);
            HdfsDataIngestion hdfsDataIngestion = new HdfsDataIngestion(config);
            Thread.sleep(10000);
            hdfsDataIngestion.run();

            // Assert that the kafka records were ingested and pruned correctly and the database holds only the expected 10 files.
            Assertions
                    .assertEquals(10, fs.listStatus(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertFalse(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "1.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "2.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "3.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "4.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "5.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "6.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "7.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "8.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "9.13")));
        });
    }
}
