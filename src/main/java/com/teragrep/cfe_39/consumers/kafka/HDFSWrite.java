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
package com.teragrep.cfe_39.consumers.kafka;

import com.teragrep.cfe_39.configuration.ConfigurationImpl;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public final class HDFSWrite implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSWrite.class);
    private final String fileName;
    private final String path;
    private final ConfigurationImpl configuration;

    public HDFSWrite(ConfigurationImpl config, String topic, String partition, long offset) {
        this.configuration = config;
        path = config.valueOf("hdfsPath") + "/" + topic;
        fileName = partition + "." + offset; // filename should be constructed from partition and offset.
    }

    // Method for committing the AVRO-file to HDFS
    public void commit(File syslogFile) throws IOException {
        // The code for writing the file to HDFS should be same for both test (non-kerberized access) and prod (kerberized access).
        FileSystemFactoryImpl fileSystemFactoryImpl = new FileSystemFactoryImpl(configuration);
        FileSystem fs = fileSystemFactoryImpl.create(false);
        //==== Create directory if not exists
        Path workingDir = fs.getWorkingDirectory();
        // Sets the directory where the data should be stored, if the directory doesn't exist then it's created.
        Path newDirectoryPath = new Path(path);
        if (!fs.exists(newDirectoryPath)) {
            // Create new Directory
            fs.mkdirs(newDirectoryPath);
            LOGGER.info("Path <{}> created.", path);
        }

        //==== Write file
        LOGGER.debug("Begin Write file into hdfs");
        //Create a path
        Path hdfswritepath = new Path(newDirectoryPath.toString() + "/" + fileName); // filename should be set according to the requirements: 0.12345 where 0 is Kafka partition and 12345 is Kafka offset.
        if (fs.exists(hdfswritepath)) {
            LOGGER
                    .debug(
                            "Deleting the seemingly duplicate source file {} because target file {} already exists in HDFS",
                            syslogFile.getPath(), hdfswritepath
                    );
            syslogFile.delete();
            throw new RuntimeException("File " + fileName + " already exists");
        }
        else {
            LOGGER.debug("Target file <{}> doesn't exist, proceeding normally.", hdfswritepath);
        }

        Path path = new Path(syslogFile.getPath());
        fs.copyFromLocalFile(path, hdfswritepath);
        LOGGER.debug("End Write file into hdfs");
        LOGGER.info("\nFile committed to HDFS, file writepath should be: <{}>\n", hdfswritepath);
    }

    // try-with-resources handles closing the filesystem automatically.
    public void close() {
        /* NoOp
         When used here fs.close() doesn't just affect the current class, it affects all the FileSystem objects that were created using FileSystem.get(URI.create(hdfsuri), conf); in different threads.*/
    }

}
