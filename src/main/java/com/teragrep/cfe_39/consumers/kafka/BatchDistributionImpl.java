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
import com.teragrep.cfe_39.configuration.Config;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import com.teragrep.cfe_39.metrics.DurationStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Instant;
import java.util.*;

/*  The kafka stream should first be deserialized using rlo_06 and then serialized again using avro and stored in HDFS.
  The target where the record is stored in HDFS is based on the topic, partition and offset. ie. topic_name/0.123456 where offset is 123456
 The mock consumer is activated for testing using the configuration file: readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")*/

public class BatchDistributionImpl implements BatchDistribution {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchDistributionImpl.class);

    private final String topic;
    private final DurationStatistics durationStatistics;
    private final TopicCounter topicCounter;
    private long lastTimeCalled;
    private final Config config;
    private final Map<String, PartitionFileImpl> partitionFileMap;

    public BatchDistributionImpl(
            Config config,
            String topic,
            DurationStatistics durationStatistics,
            TopicCounter topicCounter
    ) {
        this.config = config;
        this.topic = topic;
        this.durationStatistics = durationStatistics;
        this.topicCounter = topicCounter;
        this.partitionFileMap = new HashMap<>();
        this.lastTimeCalled = Instant.now().toEpochMilli();
    }

    /* Input parameter is a batch of RecordOffsetObjects from kafka. Each object contains a record and its metadata (topic, partition and offset).
    * Distributes the received kafka record batch to PartitionFileImpl objects based on topic partition which the record originates from.
    * */
    @Override
    public void accept(List<KafkaRecordImpl> batch) {
        long thisTime = Instant.now().toEpochMilli();
        long ftook = thisTime - lastTimeCalled;
        topicCounter.setKafkaLatency(ftook);
        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .debug(
                            "Fuura searching your batch for <[{}]> with records <{}> and took  <{}> milliseconds. <{}> EPS. ",
                            topic, batch.size(), (ftook), (batch.size() * 1000L / ftook)
                    );
        }
        long batchBytes = 0L;
        long start = Instant.now().toEpochMilli();
        // Starts measuring performance here. Measures how long it takes to process the whole batch.

        // Distribute the records of the batch to a PartitionFileImpl object based on partition from which the record originates from.
        ListIterator<KafkaRecordImpl> recordOffsetListIterator = batch.listIterator();
        while (recordOffsetListIterator.hasNext()) {
            KafkaRecordImpl next = recordOffsetListIterator.next();
            JsonObject recordOffset = JsonParser.parseString(next.offsetToJSON()).getAsJsonObject();
            // If the PartitionFileImpl corresponding to the record's partition doesn't exist, create one.
            if (!partitionFileMap.containsKey(recordOffset.get("partition").getAsString())) {
                try {
                    partitionFileMap
                            .put(recordOffset.get("partition").getAsString(), new PartitionFileImpl(config, recordOffset));
                }
                catch (IOException e) {
                    LOGGER.error("Failed to create new PartitionFileImpl for record <{}>", recordOffset);
                    throw new RuntimeException(e);
                }
            }
            // Every PartitionFileImpl object will hold responsibility over a single unique file that is related to a single topic partition.
            PartitionFileImpl recordPartitionFile = partitionFileMap.get(recordOffset.get("partition").getAsString());
            // Tell PartitionFileImpl to add the current record to the list of records that are going to be added to the file.
            recordPartitionFile.addRecord(next);
            batchBytes = batchBytes + next.size(); // metrics
        }

        // When all records in the current batch have been distributed to different PartitionFileImpl objects successfully, proceed to adding the records to the files for all PartitionFileImpl objects.
        partitionFileMap.forEach((key, value) -> {
            try {
                value.commitRecords();
            }
            catch (IOException e) {
                LOGGER.error("Failed to write the SyslogRecords to PartitionFileImpl <{}> in topic <{}>", key, topic);
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
        long rps = batch.size() * 1000L / took;
        topicCounter.setRecordsPerSecond(rps);
        long bps = batchBytes * 1000 / took;
        topicCounter.setBytesPerSecond(bps);
        durationStatistics.addAndGetRecords(batch.size());
        durationStatistics.addAndGetBytes(batchBytes);
        topicCounter.addToTotalBytes(batchBytes);
        topicCounter.addToTotalRecords(batch.size());
        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .debug(
                            "Sent batch for <[{}]> with records <{}> and size <{}> KB took <{}> milliseconds. <{}> RPS. <{}> KB/s ",
                            topic, batch.size(), batchBytes / 1024, (took), rps, bps / 1024
                    );
        }
        lastTimeCalled = Instant.now().toEpochMilli();
    }

    @Override
    public void rebalance() {
        // Handle rebalancing here. Store all remaining records of all PartitionFile objects to HDFS.
        accept(new ArrayList<>()); // Will write all files with records still in them to HDFS.
        // Delete all PartitionFile objects from the partitionFileMap. Must also delete the files linked to the objects.
        partitionFileMap.forEach((key, value) -> {
            value.delete();
        });
        partitionFileMap.clear();
    }
}
