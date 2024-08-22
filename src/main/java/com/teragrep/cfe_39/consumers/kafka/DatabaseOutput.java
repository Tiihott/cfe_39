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

import com.google.gson.*;
import com.teragrep.cfe_39.Config;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import com.teragrep.cfe_39.metrics.DurationStatistics;
import com.teragrep.rlo_06.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

/*  The kafka stream should first be deserialized using rlo_06 and then serialized again using avro and stored in HDFS.
  The target where the record is stored in HDFS is based on the topic, partition and offset. ie. topic_name/0.123456 where offset is 123456
 The mock consumer is activated for testing using the configuration file: readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")*/

public class DatabaseOutput implements Consumer<List<KafkaRecordImpl>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseOutput.class);
    private final String topic;
    private final DurationStatistics durationStatistics;
    private final TopicCounter topicCounter;
    private long lastTimeCalled;
    private final Config config;
    private final boolean skipNonRFC5424Records;
    private final boolean skipEmptyRFC5424Records;
    private final Map<String, PartitionFile> partitionFileMap;

    // BatchDistribution? RecordDistribution?
    public DatabaseOutput(
            Config config,
            String topic,
            DurationStatistics durationStatistics,
            TopicCounter topicCounter
    ) {
        this.config = config;
        this.topic = topic;
        this.durationStatistics = durationStatistics;
        this.topicCounter = topicCounter;
        this.skipNonRFC5424Records = config.getSkipNonRFC5424Records();
        this.skipEmptyRFC5424Records = config.getSkipEmptyRFC5424Records();
        this.partitionFileMap = new HashMap<>();

        this.lastTimeCalled = Instant.now().toEpochMilli();
    }

    /* Input parameter is a batch of RecordOffsetObjects from kafka. Each object contains a record and its metadata (topic, partition and offset).
     Each partition will get their set of exclusive AVRO-files in HDFS.
     The target where the record is stored in HDFS is based on the topic, partition and last offset. ie. topic_name/0.123456 where last written record's offset is 123456.
     AVRO-file with a path/name that starts with topic_name/0.X should only contain records from the 0th partition of topic named topic_name, topic_name/1.X should only contain records from 1st partition, etc.
     AVRO-files are created dynamically, thus it is not known which record (and its offset) is written to the file last before committing it to HDFS. The final name for the HDFS file is decided only when the file is committed to HDFS.*/
    @Override
    public void accept(List<KafkaRecordImpl> recordOffsetObjectList) {
        long thisTime = Instant.now().toEpochMilli();
        long ftook = thisTime - lastTimeCalled;
        topicCounter.setKafkaLatency(ftook);
        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .debug(
                            "Fuura searching your batch for <[{}]> with records <{}> and took  <{}> milliseconds. <{}> EPS. ",
                            topic, recordOffsetObjectList.size(), (ftook),
                            (recordOffsetObjectList.size() * 1000L / ftook)
                    );
        }
        long batchBytes = 0L;

        /*  The recordOffsetObjectList loop will go through all the objects in the list.
          The objects can serialize their contents into SyslogRecords that can be stored to an AVRO-file.
          When the file size is about to go above 64M, commit the file into HDFS using the latest topic/partition/offset values as the filename and start fresh with a new empty AVRO-file.
          Serialize the object that was going to make the file go above 64M into the now empty AVRO-file.
          .*/
        long start = Instant.now().toEpochMilli(); // Starts measuring performance here. Measures how long it takes to process the whole recordOffsetObjectList.
        ListIterator<KafkaRecordImpl> recordOffsetListIterator = recordOffsetObjectList.listIterator();
        while (recordOffsetListIterator.hasNext()) {
            // process recordOffsetObjectList here, the consumer only consumes 500 records in a single batch so the file can't be committed during a single accept().
            // Distribute the records to a PartitionFile object based on partition from which the record originates from.

            // load the next KafkaRecord
            KafkaRecordImpl next = recordOffsetListIterator.next();
            // Read the topic, partition and offset information of the record
            JsonObject recordOffset = JsonParser.parseString(next.offsetToJSON()).getAsJsonObject();
            String topic = recordOffset.get("topic").getAsString();
            String partition = recordOffset.get("partition").getAsString();
            // Pass the record to the PartitionFile object that it belongs to. If the correct PartitionFile doesn't exist, create one.
            if (!partitionFileMap.containsKey(partition)) {
                try {
                    partitionFileMap.put(partition, new PartitionFile(config, topic, partition));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            // Every PartitionFile object will hold responsibility over a single unique file that is related to a single topic partition.
            PartitionFile recordPartitionFile = partitionFileMap.get(partition);
            // Tell PartitionFile to add the current record to the list of records that are going to be added to the file. Handle skipping of broken records.
            try {
                recordPartitionFile.addRecord(next.toSyslogRecord());
            }
            catch (ParseException e) {
                if (skipNonRFC5424Records) {
                    LOGGER
                            .warn(
                                    "Skipping parsing a non RFC5424 record, record metadata: <{}>. Exception information: ",
                                    recordOffset, e
                            );
                }
                else {
                    throw new RuntimeException(e);
                }
            }
            catch (NullPointerException e) {
                if (skipEmptyRFC5424Records) {
                    LOGGER
                            .warn(
                                    "Skipping parsing an empty RFC5424 record, record metadata: <{}>. Exception information: ",
                                    recordOffset, e
                            );
                }
                else {
                    throw new RuntimeException(e);
                }
            }
        }

        // When all records in the current batch have been distributed to different PartitionFile objects successfully, commit the adding of records to the files for all PartitionFile objects.
        partitionFileMap.forEach((key, value) -> {
            try {
                value.commitRecords();
            }
            catch (IOException e) {
                LOGGER.error("Failed to write the SyslogRecords to PartitionFile <{}> in topic <{}>", key, topic);
                // FIXME: Delete the files that were stored to HDFS before the exception hit, to make sure data integrity is preserved during consumer rebalance as kafka consumer will not mark the failed record batch as committed.
                throw new RuntimeException(e);
            }
        });

        // Measures performance of code that is between start and end.
        long end = Instant.now().toEpochMilli();
        long took = (end - start);
        topicCounter.setDatabaseLatency(took);
        if (took == 0) {
            took = 1;
        }
        long rps = recordOffsetObjectList.size() * 1000L / took;
        topicCounter.setRecordsPerSecond(rps);
        long bps = batchBytes * 1000 / took;
        topicCounter.setBytesPerSecond(bps);
        durationStatistics.addAndGetRecords(recordOffsetObjectList.size());
        durationStatistics.addAndGetBytes(batchBytes);
        topicCounter.addToTotalBytes(batchBytes);
        topicCounter.addToTotalRecords(recordOffsetObjectList.size());
        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .debug(
                            "Sent batch for <[{}]> with records <{}> and size <{}> KB took <{}> milliseconds. <{}> RPS. <{}> KB/s ",
                            topic, recordOffsetObjectList.size(), batchBytes / 1024, (took), rps, bps / 1024
                    );
        }
        lastTimeCalled = Instant.now().toEpochMilli();
    }
}
