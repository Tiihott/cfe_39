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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {

    private final Logger LOGGER = LoggerFactory.getLogger(ConsumerRebalanceListenerImpl.class);
    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final BatchDistributionImpl callbackFunction;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    public ConsumerRebalanceListenerImpl(
            Consumer<byte[], byte[]> kafkaConsumer,
            BatchDistributionImpl callbackFunction
    ) {
        this.kafkaConsumer = kafkaConsumer;
        this.callbackFunction = callbackFunction;
        this.currentOffsets = new HashMap<>();
    }

    public void addOffsetToTrack(String topic, int partition, long offset) {
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1, null));
        // 1. Pass listener to callbackFunction.
        // 2. Call the addOffsetToTrack() every time a file is stored to HDFS.
        // 3. Finally remove the try/catch from BatchDistributionImpl and instead let the KafkaReader to try/catch the exception.
        // 4. In KafkaReader commit the offsets using the listener's getCurrentOffsets() method, and then re-throw the exception.
    }

    // this is used when we shut down our consumer gracefully
    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        // Flush any records from the temporary files to HDFS to synchronize database with committed kafka offsets, and clean up PartitionFile list.
        LOGGER.info("onPartitionsRevoked triggered");
        callbackFunction.rebalance();
        LOGGER.info("Committing offsets <{}>", currentOffsets);
        kafkaConsumer.commitSync(currentOffsets);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        LOGGER.info("onPartitionsAssigned triggered");
        // NoOp: records and offsets are already stored to HDFS by the callbackFunction.rebalance(), and kafka coordinator should handle committed offsets automatically.
    }
}
