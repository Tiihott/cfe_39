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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.teragrep.cfe_39.configuration.CommonConfiguration;
import com.teragrep.cfe_39.configuration.HdfsConfiguration;
import com.teragrep.cfe_39.consumers.kafka.HDFSWrite;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

// Tests the functionality of the HDFSWrite.java.
public class HdfsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsTest.class);

    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static CommonConfiguration config;
    private static HdfsConfiguration hdfsConfig;
    private FileSystem fs;

    // Start minicluster and initialize config.
    @BeforeEach
    public void startMiniCluster() {
        assertDoesNotThrow(() -> {
            Map<String, String> map = new HashMap<>();
            map.put("log4j2.configurationFile", "/opt/teragrep/cfe_39/etc/log4j2.properties");
            map.put("egress.configurationFile", "/opt/teragrep/cfe_39/etc/egress.properties");
            map.put("ingress.configurationFile", "/opt/teragrep/cfe_39/etc/ingress.properties");
            map.put("queueDirectory", System.getProperty("user.dir") + "/etc/AVRO/");
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
            hdfsMap.put("hadoop.security.authentication", "kerberos");
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
    public void hdfsWriteTest() {
        // This test case is for testing the functionality of the HDFSWrite.java by writing pre-generated AVRO-files to the HDFS database and asserting the results are correct.
        assertDoesNotThrow(() -> {
            Assertions.assertFalse(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic")));

            String pathname = System.getProperty("user.dir") + "/src/test/resources/mockHdfsFiles/0.9";
            java.nio.file.Path sourceFile = Paths.get(pathname);
            java.nio.file.Path targetDir = Paths.get(config.queueDirectory());
            java.nio.file.Path targetFile = targetDir.resolve(sourceFile.getFileName());
            Assertions.assertFalse(targetFile.toFile().exists());
            Files.copy(sourceFile, targetFile);
            Assertions.assertTrue(targetFile.toFile().exists());
            File avroFile = new File(targetFile.toUri());
            JsonObject recordOffsetJo = JsonParser
                    .parseString("{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":9}")
                    .getAsJsonObject();
            try (HDFSWrite writer = new HDFSWrite(hdfsConfig, "testConsumerTopic", "0", 9)) {
                writer.commit(avroFile); // commits avroFile to HDFS.
            }
            targetFile.toFile().delete(); // writer no longer handles deletion of the files
            Assertions.assertFalse(targetFile.toFile().exists());
            Assertions
                    .assertEquals(1, fs.listStatus(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));

            pathname = System.getProperty("user.dir") + "/src/test/resources/mockHdfsFiles/0.13";
            sourceFile = Paths.get(pathname);
            targetDir = Paths.get(config.queueDirectory());
            targetFile = targetDir.resolve(sourceFile.getFileName());
            Files.copy(sourceFile, targetFile);
            Assertions.assertTrue(targetFile.toFile().exists());
            avroFile = new File(config.queueDirectory() + "/0.13");
            recordOffsetJo = JsonParser
                    .parseString("{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":13}")
                    .getAsJsonObject();
            try (HDFSWrite writer = new HDFSWrite(hdfsConfig, "testConsumerTopic", "0", 13)) {
                writer.commit(avroFile); // commits avroFile to HDFS and deletes avroFile afterward.
            }
            targetFile.toFile().delete(); // writer no longer handles deletion of the files
            Assertions.assertFalse(targetFile.toFile().exists());
            Assertions
                    .assertEquals(2, fs.listStatus(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
            Assertions
                    .assertTrue(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic" + "/" + "0.13")));
        });
    }

    @Test
    public void hdfsWriteExceptionTest() {
        // This test case is for testing the functionality of the HDFSWrite.java exception handling by trying to write the same file twice and asserting that the proper exception is thrown.
        assertDoesNotThrow(() -> {
            Assertions.assertFalse(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic")));

            String pathname = System.getProperty("user.dir") + "/src/test/resources/mockHdfsFiles/0.9";
            java.nio.file.Path sourceFile = Paths.get(pathname);
            java.nio.file.Path targetDir = Paths.get(config.queueDirectory());
            java.nio.file.Path targetFile = targetDir.resolve(sourceFile.getFileName());
            Assertions.assertFalse(targetFile.toFile().exists());
            Files.copy(sourceFile, targetFile);
            Assertions.assertTrue(targetFile.toFile().exists());
            File avroFile = new File(targetFile.toUri());
            JsonObject recordOffsetJo = JsonParser
                    .parseString("{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":9}")
                    .getAsJsonObject();
            try (HDFSWrite writer = new HDFSWrite(hdfsConfig, "testConsumerTopic", "0", 9)) {
                writer.commit(avroFile); // commits avroFile to HDFS.
            }
            targetFile.toFile().delete(); // writer no longer handles deletion of the files
            Assertions.assertFalse(targetFile.toFile().exists());
            Assertions
                    .assertEquals(1, fs.listStatus(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));

            Files.copy(sourceFile, targetFile);
            Assertions.assertTrue(targetFile.toFile().exists());
            avroFile = new File(config.queueDirectory() + "/0.9");
            recordOffsetJo = JsonParser
                    .parseString("{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":9}")
                    .getAsJsonObject();
            HDFSWrite writer = new HDFSWrite(hdfsConfig, "testConsumerTopic", "0", 9);
            File finalAvroFile = avroFile;
            Exception e = Assertions.assertThrows(Exception.class, () -> writer.commit(finalAvroFile));
            Assertions.assertEquals("File 0.9 already exists", e.getMessage());
            writer.close();
            targetFile.toFile().delete(); // writer no longer handles deletion of the files
            Assertions.assertFalse(targetFile.toFile().exists());
            Assertions
                    .assertEquals(1, fs.listStatus(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(hdfsConfig.hdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
        });
    }
}
