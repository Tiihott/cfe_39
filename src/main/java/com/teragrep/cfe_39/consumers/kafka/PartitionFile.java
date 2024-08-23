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
import com.google.gson.JsonParser;
import com.teragrep.cfe_39.Config;
import com.teragrep.cfe_39.avro.SyslogRecord;
import com.teragrep.cfe_39.consumers.kafka.queue.WritableQueue;
import com.teragrep.rlo_06.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class PartitionFile {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionFile.class);

    private final String topic;
    private final String partition;
    private final Config config;
    private final WritableQueue writableQueue;
    private final File syslogFile;
    private final List<KafkaRecordImpl> kafkaRecordList;

    PartitionFile(Config config, String topic, String partition) throws IOException {
        this.writableQueue = new WritableQueue(config.getQueueDirectory(), topic + partition);
        this.syslogFile = writableQueue.getNextWritableFile();
        this.kafkaRecordList = new ArrayList<>();
        this.config = config;
        this.topic = topic;
        this.partition = partition;
    }

    public void addRecord(KafkaRecordImpl kafkaRecord) {
        kafkaRecordList.add(kafkaRecord);
    }

    public void commitRecords() throws IOException {
        ListIterator<KafkaRecordImpl> kafkaRecordListIterator = kafkaRecordList.listIterator();
        long storedOffset = 0;
        while (kafkaRecordListIterator.hasNext()) {
            KafkaRecordImpl next = kafkaRecordListIterator.next();
            SyslogRecord syslogRecord;
            try {
                syslogRecord = next.toSyslogRecord();
            }
            catch (ParseException e) {
                if (config.getSkipNonRFC5424Records()) {
                    LOGGER
                            .warn(
                                    "Skipping parsing a non RFC5424 record, record metadata: <{}>. Exception information: ",
                                    next.offsetToJSON(), e
                            );
                    JsonObject recordOffset = JsonParser.parseString(next.offsetToJSON()).getAsJsonObject();
                    if (recordOffset.get("offset").getAsLong() > storedOffset) {
                        storedOffset = recordOffset.get("offset").getAsLong();
                    }
                    continue;
                }
                else {
                    LOGGER.error("Failed to parse RFC5424 record <{}>", next.offsetToJSON());
                    throw new RuntimeException(e);
                }
            }
            catch (NullPointerException e) {
                if (config.getSkipEmptyRFC5424Records()) {
                    LOGGER
                            .warn(
                                    "Skipping parsing an empty RFC5424 record, record metadata: <{}>. Exception information: ",
                                    next.offsetToJSON(), e
                            );
                    JsonObject recordOffset = JsonParser.parseString(next.offsetToJSON()).getAsJsonObject();
                    if (recordOffset.get("offset").getAsLong() > storedOffset) {
                        storedOffset = recordOffset.get("offset").getAsLong();
                    }
                    continue;
                }
                else {
                    LOGGER.error("Failed to parse RFC5424 record <{}> because of null content", next.offsetToJSON());
                    throw new RuntimeException(e);
                }
            }
            long syslogRecordCapacity = syslogRecord.toByteBuffer().capacity();
            long syslogFileCapacity = syslogFile.length();
            // When the file size is about to go above 64M, commit the file into HDFS using the latest topic/partition/offset values as the filename and start fresh with a new empty AVRO-file.
            if (config.getMaximumFileSize() < (syslogFileCapacity + syslogRecordCapacity)) {
                writeToHdfs(topic, partition, storedOffset);
            }
            // SyslogAvroWriter initialization will re-initialize the syslogFile if it has been deleted because of writeToHdfs().
            try (SyslogAvroWriter syslogAvroWriter = new SyslogAvroWriter(syslogFile)) {
                syslogAvroWriter.write(syslogRecord);
            }
            if (syslogRecord.getOffset() > storedOffset) {
                storedOffset = syslogRecord.getOffset();
            }
        }
        // Clear the kafkaRecordList from successfully committed records.
        kafkaRecordList.clear();
    }

    // Writes the file to hdfs and initializes new file.
    public void writeToHdfs(String topic, String partition, long offset) throws IOException {
        try (HDFSWrite writer = new HDFSWrite(config, topic, partition, offset)) {
            writer.commit(syslogFile); // commits the final AVRO-file to HDFS.
        }
        syslogFile.delete(); // Deletes the file as all the contents have been stored to HDFS.
    }

}
