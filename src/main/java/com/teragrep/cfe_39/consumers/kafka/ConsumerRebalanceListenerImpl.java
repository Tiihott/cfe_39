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

import com.teragrep.cfe_39.configuration.NewHdfsConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public final class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {

    private final Logger LOGGER = LoggerFactory.getLogger(ConsumerRebalanceListenerImpl.class);

    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final BatchDistributionImpl callbackFunction;
    private final NewHdfsConfiguration config;

    public ConsumerRebalanceListenerImpl(
            Consumer<byte[], byte[]> kafkaConsumer,
            BatchDistributionImpl callbackFunction,
            NewHdfsConfiguration config
    ) {
        this.kafkaConsumer = kafkaConsumer;
        this.callbackFunction = callbackFunction;
        this.config = config;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // Flush any records from the temporary files to HDFS to synchronize database with committed kafka offsets, and clean up PartitionFile list.
        LOGGER.info("onPartitionsRevoked triggered");
        callbackFunction.rebalance();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Generates offsets of the already committed records for Kafka and passes them to the kafka consumers.
        LOGGER.info("onPartitionsAssigned triggered");
        // Initialize FileSystem
        FileSystemFactoryImpl fileSystemFactoryImpl = new FileSystemFactoryImpl(config);
        FileSystem fs;
        try {
            fs = fileSystemFactoryImpl.create(false);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        Map<TopicPartition, Long> hdfsStartOffsets = new HashMap<>();
        try (HDFSRead hr = new HDFSRead(config, fs)) {
            hdfsStartOffsets = hr.hdfsStartOffsets();
            LOGGER.debug("topicPartitionStartMap generated succesfully: <{}>", hdfsStartOffsets);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (TopicPartition topicPartition : partitions) {
            if (hdfsStartOffsets.containsKey(topicPartition)) {
                long position = kafkaConsumer.position(topicPartition);
                if (position < hdfsStartOffsets.get(topicPartition)) {
                    kafkaConsumer.seek(topicPartition, hdfsStartOffsets.get(topicPartition));
                }
            }
        }
    }
}
