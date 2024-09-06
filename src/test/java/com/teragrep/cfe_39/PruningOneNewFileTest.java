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

import com.teragrep.cfe_39.configuration.ConfigurationImpl;
import com.teragrep.cfe_39.consumers.kafka.HDFSPrune;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class PruningOneNewFileTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PruningOneNewFileTest.class);
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
            config = new ConfigurationImpl().loadPropertiesFile();
            // Create a HDFS miniCluster
            baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
            hdfsCluster = new TestMiniClusterFactory().create(config, baseDir);
            config = config.with("hdfsuri", "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/");
            config = config.with("queueDirectory", System.getProperty("user.dir") + "/etc/AVRO/");
            config = config
                    .with("log4j2.configurationFile", System.getProperty("user.dir") + "/rpm/resources/log4j2.properties");
            config.configureLogging();
            fs = new TestFileSystemFactory().create(config.valueOf("hdfsuri"));

            // Inserts a single pre-made avro-file with a new timestamp to HDFS, which is normally generated during data ingestion from mock kafka consumer.
            String path = config.valueOf("hdfsPath") + "/" + "testConsumerTopic"; // "hdfs:///opt/teragrep/cfe_39/srv/testConsumerTopic"
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
            String fileName = "0.9";
            Assertions.assertTrue(listOfFiles.contains(fileName));
            String pathname = dir + "/" + fileName;
            File avroFile = new File(pathname);
            //==== Write file
            LOGGER.debug("Begin Write file into hdfs");
            //Create a path
            Path hdfswritepath = new Path(newDirectoryPath + "/" + avroFile.getName()); // filename should be set according to the requirements: 0.12345 where 0 is Kafka partition and 12345 is Kafka offset.
            Assertions.assertFalse(fs.exists(hdfswritepath));
            Path readPath = new Path(avroFile.getPath());
            fs.copyFromLocalFile(readPath, hdfswritepath);
            fs.setTimes(hdfswritepath, -1, -1);
            LOGGER.debug("End Write file into hdfs");
            LOGGER.debug("\nFile committed to HDFS, file writepath should be: {}\n", hdfswritepath);
        });

    }

    // Teardown the minicluster
    @AfterEach
    public void teardownMiniCluster() {
        assertDoesNotThrow(fs::close);
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }

    @Test
    public void oneNewFileTest() {
        // This test case is for testing the functionality of the HDFSPrune.java when the database holds a file with a timestamp that shouldn't trigger pruning of old files.
        Assertions.assertTrue(Long.parseLong(config.valueOf("pruneOffset")) >= 300000L); // Fails the test if the config is not correct, too low pruning offset can prune the files if the test is lagging.
        Assertions
                .assertTrue(System.currentTimeMillis() - Long.parseLong(config.valueOf("pruneOffset")) > 157784760000L);

        assertDoesNotThrow(() -> {
            Assertions.assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic")));
            Assertions
                    .assertEquals(1, fs.listStatus(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic")).length);
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "0.9")));
            HDFSPrune hdfsPrune = new HDFSPrune(config, "testConsumerTopic", fs);
            int deleted = hdfsPrune.prune();
            Assertions.assertEquals(0, deleted);
            // Also check with HDFS access if expected files still exist.
            Assertions
                    .assertEquals(1, fs.listStatus(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic")).length);
            Assertions
                    .assertTrue(fs.exists(new Path(config.valueOf("hdfsPath") + "/" + "testConsumerTopic" + "/" + "0.9")));
        });
    }
}
