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

import com.teragrep.cfe_39.consumers.kafka.HDFSPrune;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class PruningTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CombinedFullTest.class);
    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static Config config;
    private FileSystem fs;

    // Start minicluster and initialize config.
    @BeforeEach
    public void startMiniCluster() {
        assertDoesNotThrow(() -> {
            config = new Config();
            // Create a HDFS miniCluster
            baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
            Configuration conf = new Configuration();
            conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
            MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
            hdfsCluster = builder.build();
            String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
            config.setHdfsuri(hdfsURI);
            DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();

            // ====== Init HDFS File System Object
            Configuration fsConf = new Configuration();
            // Set FileSystem URI
            fsConf.set("fs.defaultFS", hdfsURI);
            // Because of Maven
            fsConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            fsConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            // Set HADOOP user
            System.setProperty("HADOOP_USER_NAME", "hdfs");
            System.setProperty("hadoop.home.dir", "/");
            String path = config.getHdfsPath() + "/" + "testConsumerTopic"; // "hdfs:///opt/teragrep/cfe_39/srv/testConsumerTopic"
            fs = FileSystem.get(URI.create(hdfsURI), fsConf);
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

    /* TODO: Create more extensive tests based on happyTest:
     - HDFS has 1 file already inside it that is timestamped as young enough that it is not pruned.
     - HDFS has 2 files already inside it that are timestamped as young enough that they are not pruned. happyTest()
     - HDFS has 1 file already inside it that is timestamped as old enough for pruning to trigger.
     - HDFS has 2 files already inside it that are timestamped as old enough for pruning to trigger.
     - HDFS has 2 files already inside it where one file is old enough to be pruned and the other one is not.*/

    @Test
    public void happyTest() {
        // Test for not triggering pruning for files in the topic.
        Assertions.assertTrue(config.getPruneOffset() >= 300000L); // Fails the test if the config is not correct, too low pruning offset can prune the files if the test is lagging.
        insertMockFiles(-1, -1); // Insert 2 mock files normally with young timestamps so pruning should not trigger on them.

        assertDoesNotThrow(() -> {
            HDFSPrune hdfsPrune = new HDFSPrune(config, "testConsumerTopic");
            int deleted = hdfsPrune.prune();
            Assertions.assertEquals(0, deleted);
            // Also check with HDFS access if expected files still exist.
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.13")));
        });
    }

    // Inserts pre-made avro-files to HDFS, which are normally generated during data ingestion from mock kafka consumer.
    private void insertMockFiles(long a, long b) {
        String path = config.getHdfsPath() + "/" + "testConsumerTopic"; // "hdfs:///opt/teragrep/cfe_39/srv/testConsumerTopic"
        //Get the filesystem - HDFS
        assertDoesNotThrow(() -> {

            //==== Create directory if not exists
            Path workingDir = fs.getWorkingDirectory();
            // Sets the directory where the data should be stored, if the directory doesn't exist then it's created.
            Path newDirectoryPath = new Path(path);
            if (!fs.exists(newDirectoryPath)) {
                // Create new Directory
                fs.mkdirs(newDirectoryPath);
                LOGGER.debug("Path {} created.", path);
            }

            String dir = System.getProperty("user.dir") + "/src/test/java/com/teragrep/cfe_39/mockHdfsFiles";
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
                if (fs.exists(hdfswritepath)) {
                    Assertions.fail("File " + avroFile.getName() + " already exists");
                }
                Path readPath = new Path(avroFile.getPath());
                // Add conditions if file filtering is required for tests.
                fs.copyFromLocalFile(readPath, hdfswritepath);
                // Set a/b to something like 157784760000 to trigger pruning.
                if (Objects.equals(hdfswritepath.toString(), "hdfs:/opt/teragrep/cfe_39/srv/testConsumerTopic/0.9")) {
                    fs.setTimes(hdfswritepath, a, -1);
                }
                else if (
                    Objects.equals(hdfswritepath.toString(), "hdfs:/opt/teragrep/cfe_39/srv/testConsumerTopic/0.13")
                ) {
                    fs.setTimes(hdfswritepath, b, -1);
                }
                LOGGER.debug("End Write file into hdfs");
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("\nFile committed to HDFS, file writepath should be: {}\n", hdfswritepath.toString());
                }
            }
        });
    }

}
