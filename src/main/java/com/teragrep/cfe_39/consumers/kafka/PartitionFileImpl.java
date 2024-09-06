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

import com.google.gson.JsonObject;
import com.teragrep.cfe_39.avro.SyslogRecord;
import com.teragrep.cfe_39.configuration.ConfigurationImpl;
import com.teragrep.cfe_39.consumers.kafka.queue.WritableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PartitionFileImpl implements PartitionFile {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionFileImpl.class);

    private final JsonObject topicPartition;
    private final ConfigurationImpl config;
    private final File syslogFile;
    private final List<Long> batchOffsets;
    private final PartitionRecordsImpl partitionRecords;

    PartitionFileImpl(ConfigurationImpl config, JsonObject topicPartition) throws IOException {
        WritableQueue writableQueue = new WritableQueue(
                config.valueOf("queueDirectory"),
                topicPartition.get("topic").getAsString() + topicPartition.get("partition").getAsString()
        );
        this.syslogFile = writableQueue.getNextWritableFile();
        this.config = config;
        this.topicPartition = topicPartition;
        this.batchOffsets = new ArrayList<>();
        this.partitionRecords = new PartitionRecordsImpl(config);
        try (SyslogAvroWriter syslogAvroWriter = new SyslogAvroWriter(syslogFile)) {
            // Initializes the syslogFile.
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .debug(
                            "PartitionFileImpl representing topic {} partition {} initialized successfully. syslogFile allocated to the object is located at {}",
                            topicPartition.get("topic").getAsString(), topicPartition.get("partition").getAsString(), syslogFile.getPath()
                    );
        }
    }

    @Override
    public void addRecord(KafkaRecordImpl kafkaRecord) {
        partitionRecords.addRecord(kafkaRecord);
    }

    @Override
    public void commitRecords() throws IOException {
        List<SyslogRecord> syslogRecordList = partitionRecords.toSyslogRecordList();
        long storedOffset = 0;
        for (SyslogRecord next : syslogRecordList) {
            try (SyslogAvroWriter syslogAvroWriter = new SyslogAvroWriter(syslogFile)) {
                syslogAvroWriter.write(next);
            }
            if (next.getOffset() > storedOffset) {
                storedOffset = next.getOffset();
            }
            // When the file size has gone above the maximum, commit the file into HDFS using the latest topic/partition/offset values as the filename and then delete the local avro-file.
            if (Long.parseLong(config.valueOf("maximumFileSize")) < syslogFile.length()) {
                writeToHdfs(storedOffset);
            }
        }
        // Store the last offset of the batch to a list.
        if (storedOffset > 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER
                        .debug(
                                "Kafka Batch for topic {} partition {} processed successfully. Final record offset of the batch was {}.",
                                topicPartition.get("topic").getAsString(), topicPartition.get("partition").getAsString(), storedOffset
                        );
            }
            batchOffsets.add(storedOffset);
        }
        // No records mean consumer group rebalance happened, write file to HDFS.
        if (syslogRecordList.isEmpty()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER
                        .debug(
                                "Kafka Batch for topic {} partition {} was empty. Final record offset of the batch was {}. Proceeding to write the existing syslogFile to HDFS.",
                                topicPartition.get("topic").getAsString(), topicPartition.get("partition").getAsString(), storedOffset
                        );
            }
            writeToHdfsEarly();
        }
    }

    @Override
    public void writeToHdfsEarly() throws IOException {
        if (!batchOffsets.isEmpty()) {
            writeToHdfs(batchOffsets.get(batchOffsets.size() - 1));
        }
    }

    @Override
    public void delete() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .debug(
                            "PartitionFileImpl-object representing topic {} partition {} was notified of consumer group rebalance. Deleting syslogFile allocated to the object at {}",
                            topicPartition.get("topic").getAsString(), topicPartition.get("partition").getAsString(), syslogFile.getPath()
                    );
        }
        syslogFile.delete();
    }

    // Writes the file to hdfs and initializes new file.
    private void writeToHdfs(long offset) throws IOException {
        try (
                HDFSWrite writer = new HDFSWrite(config, topicPartition.get("topic").getAsString(), topicPartition.get("partition").getAsString(), offset)
        ) {
            writer.commit(syslogFile); // commits the final AVRO-file to HDFS.
        }
        syslogFile.delete(); // Delete the file as all the contents have been stored to HDFS.
        try (SyslogAvroWriter syslogAvroWriter = new SyslogAvroWriter(syslogFile)) {
            // NoOp, syslogAvroWriter has initialized the empty AVRO-file.
        }
        batchOffsets.clear();
        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .debug(
                            "SyslogFile representing topic {} partition {} stored to HDFS with offset value of {}. SyslogFile allocated to the object is located at {}",
                            topicPartition.get("topic").getAsString(), topicPartition.get("partition").getAsString(), offset, syslogFile.getPath()
                    );
        }
    }

}
