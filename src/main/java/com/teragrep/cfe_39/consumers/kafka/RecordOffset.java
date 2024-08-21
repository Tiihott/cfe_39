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

import com.teragrep.cfe_39.avro.SyslogRecord;
import com.teragrep.rlo_06.Fragment;
import com.teragrep.rlo_06.RFC5424Frame;
import com.teragrep.rlo_06.RFC5424Timestamp;
import com.teragrep.rlo_06.SDVector;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

// This is the class for handling the Kafka record topic/partition/offset data that are required for HDFS storage.
public final class RecordOffset implements Offset {

    private final String topic;
    private final int partition;
    private final long offset;
    private final byte[] record;

    public RecordOffset(String topic, int partition, long offset, byte[] record) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.record = record;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public byte[] record() {
        return record;
    }

    @Override
    public long size() {
        return record.length;
    }

    @Override
    public String offsetToJSON() {
        return String
                .format("{\"topic\":\"%s\", \"partition\":%d, \"offset\":%d}", this.topic, this.partition, this.offset);
    }

    @Override
    public SyslogRecord toSyslogRecord() {
        RFC5424Frame rfc5424Frame = new RFC5424Frame(false);
        InputStream inputStream = new ByteArrayInputStream(record);
        rfc5424Frame.load(inputStream);

        Instant instant = new RFC5424Timestamp(rfc5424Frame.timestamp).toZonedDateTime().toInstant();
        long MICROS_PER_SECOND = 1000L * 1000L;
        long NANOS_PER_MICROS = 1000L;
        long sec = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);
        long epochMicros = Math.addExact(sec, instant.getNano() / NANOS_PER_MICROS);

        // input
        final byte[] source = eventToSource(rfc5424Frame);

        // origin
        final byte[] origin = eventToOrigin(rfc5424Frame);

        return SyslogRecord
                .newBuilder()
                .setTimestamp(epochMicros)
                .setPayload(rfc5424Frame.msg.toString())
                .setDirectory(rfc5424Frame.structuredData.getValue(new SDVector("teragrep@48577", "directory")).toString()).setStream(rfc5424Frame.structuredData.getValue(new SDVector("teragrep@48577", "streamname")).toString()).setHost(rfc5424Frame.hostname.toString()).setInput(new String(source, StandardCharsets.UTF_8)).setPartition(String.valueOf(partition)).setOffset(offset).setOrigin(new String(origin, StandardCharsets.UTF_8)).build();
    }

    private byte[] eventToOrigin(RFC5424Frame rfc5424Frame) {
        byte[] origin;
        Fragment originFragment = rfc5424Frame.structuredData.getValue(new SDVector("origin@48577", "hostname"));
        if (!originFragment.isStub) {
            origin = originFragment.toBytes();
        }
        else {
            origin = new byte[] {};
        }
        return origin;
    }

    private byte[] eventToSource(RFC5424Frame rfc5424Frame) {
        /*input is produced from SD element event_node_source@48577 by
         concatenating "source_module:hostname:source". in case
        if event_node_source@48577 is not available use event_node_relay@48577.
        If neither are present, use null value.*/

        ByteBuffer sourceConcatenationBuffer = ByteBuffer.allocateDirect(256 * 1024);
        sourceConcatenationBuffer.clear();

        Fragment sourceModuleFragment = rfc5424Frame.structuredData
                .getValue(new SDVector("event_node_source@48577", "source_module"));
        if (sourceModuleFragment.isStub) {
            sourceModuleFragment = rfc5424Frame.structuredData
                    .getValue(new SDVector("event_node_relay@48577", "source_module"));
        }

        byte[] source_module;
        if (!sourceModuleFragment.isStub) {
            source_module = sourceModuleFragment.toBytes();
        }
        else {
            source_module = new byte[] {};
        }

        Fragment sourceHostnameFragment = rfc5424Frame.structuredData
                .getValue(new SDVector("event_node_source@48577", "hostname"));
        if (sourceHostnameFragment.isStub) {
            sourceHostnameFragment = rfc5424Frame.structuredData
                    .getValue(new SDVector("event_node_relay@48577", "hostname"));
        }

        byte[] source_hostname;
        if (!sourceHostnameFragment.isStub) {
            source_hostname = sourceHostnameFragment.toBytes();
        }
        else {
            source_hostname = new byte[] {};
        }

        Fragment sourceSourceFragment = rfc5424Frame.structuredData
                .getValue(new SDVector("event_node_source@48577", "source"));
        if (sourceHostnameFragment.isStub) {
            sourceSourceFragment = rfc5424Frame.structuredData
                    .getValue(new SDVector("event_node_relay@48577", "source"));
        }

        byte[] source_source;
        if (!sourceSourceFragment.isStub) {
            source_source = sourceSourceFragment.toBytes();
        }
        else {
            source_source = new byte[] {};
        }

        sourceConcatenationBuffer.put(source_module);
        sourceConcatenationBuffer.put((byte) ':');
        sourceConcatenationBuffer.put(source_hostname);
        sourceConcatenationBuffer.put((byte) ':');
        sourceConcatenationBuffer.put(source_source);

        sourceConcatenationBuffer.flip();
        byte[] input = new byte[sourceConcatenationBuffer.remaining()];
        sourceConcatenationBuffer.get(input);

        return input;
    }

}
