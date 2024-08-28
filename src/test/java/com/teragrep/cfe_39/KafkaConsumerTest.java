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

import com.teragrep.cfe_39.configuration.Config;
import com.teragrep.cfe_39.consumers.kafka.BatchDistributionImpl;
import com.teragrep.cfe_39.consumers.kafka.ReadCoordinator;
import com.teragrep.cfe_39.consumers.kafka.KafkaRecordImpl;
import com.teragrep.rlo_06.ParseException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class KafkaConsumerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerTest.class);

    @Disabled(value = "This needs refactoring")
    @Test
    public void readCoordinatorTest2Threads() {
        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            Config config = new Config();
            Map<TopicPartition, Long> hdfsStartOffsets = new HashMap<>();
            ArrayList<List<KafkaRecordImpl>> messages = new ArrayList<>();
            Consumer<List<KafkaRecordImpl>> output = message -> messages.add(message); // FIXME: Lambda does not work with BatchDistributionImpl interface

            ReadCoordinator readCoordinator = new ReadCoordinator(
                    "testConsumerTopic",
                    config.getKafkaConsumerProperties(),
                    (BatchDistributionImpl) output, // FIXME: Dont/Can't use casting like this.
                    hdfsStartOffsets
            );
            Thread readThread = new Thread(null, readCoordinator, "testConsumerTopic1"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(1000);

            ReadCoordinator readCoordinator2 = new ReadCoordinator(
                    "testConsumerTopic",
                    config.getKafkaConsumerProperties(),
                    (BatchDistributionImpl) output, // FIXME: Dont/Can't use casting like this.
                    hdfsStartOffsets
            );
            Thread readThread2 = new Thread(null, readCoordinator2, "testConsumerTopic2"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread2.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(10000);
            Assertions.assertEquals(2, messages.size());
            Assertions.assertEquals(160, messages.get(0).size() + messages.get(1).size()); // Assert that expected amount of records has been consumed by the consumer group.
            Assertions.assertEquals(80, messages.get(0).size());
            Assertions.assertEquals(80, messages.get(1).size());

            // Assert that all the record contents are correct, every topic partition has identical set of offset-message pairings.
            List<String> messageList = new ArrayList<String>();
            messageList.add("[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!");
            messageList.add("[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!");
            messageList.add("470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.");
            messageList.add("470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.");
            messageList.add("470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.");
            messageList
                    .add(
                            "25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]"
                    );

            KafkaRecordImpl kafkaRecord;

            Iterator<String> iterator = messageList.iterator();
            int counter = 0;
            for (int i = 0; i <= 13; i++) {
                kafkaRecord = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":7, \"offset\":" + i + "}",
                                kafkaRecord.offsetToJSON()
                        );
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), kafkaRecord.toSyslogRecord().getPayload().toString());
                counter++;
            }

            kafkaRecord = messages.get(0).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":7, \"offset\":" + 14 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            Assertions.assertEquals(0, kafkaRecord.size());
            counter++;

            kafkaRecord = messages.get(0).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":7, \"offset\":" + 15 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            KafkaRecordImpl finalKafkaRecord = kafkaRecord;
            ParseException e = Assertions.assertThrows(ParseException.class, finalKafkaRecord::toSyslogRecord);
            Assertions.assertEquals("PRIORITY < missing", e.getMessage());
            counter++;

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                kafkaRecord = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":5, \"offset\":" + i + "}",
                                kafkaRecord.offsetToJSON()
                        );

                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), kafkaRecord.toSyslogRecord().getPayload().toString());
                counter++;
            }

            kafkaRecord = messages.get(0).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":5, \"offset\":" + 14 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            Assertions.assertEquals(0, kafkaRecord.size());
            counter++;

            kafkaRecord = messages.get(0).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":5, \"offset\":" + 15 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            KafkaRecordImpl finalKafkaRecord1 = kafkaRecord;
            e = Assertions.assertThrows(ParseException.class, finalKafkaRecord1::toSyslogRecord);
            Assertions.assertEquals("PRIORITY < missing", e.getMessage());
            counter++;

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                kafkaRecord = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":3, \"offset\":" + i + "}",
                                kafkaRecord.offsetToJSON()
                        );
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), kafkaRecord.toSyslogRecord().getPayload().toString());
                counter++;
            }

            kafkaRecord = messages.get(0).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":3, \"offset\":" + 14 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            Assertions.assertEquals(0, kafkaRecord.size());
            counter++;

            kafkaRecord = messages.get(0).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":3, \"offset\":" + 15 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            KafkaRecordImpl finalKafkaRecord2 = kafkaRecord;
            e = Assertions.assertThrows(ParseException.class, finalKafkaRecord2::toSyslogRecord);
            Assertions.assertEquals("PRIORITY < missing", e.getMessage());
            counter++;

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                kafkaRecord = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":1, \"offset\":" + i + "}",
                                kafkaRecord.offsetToJSON()
                        );
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), kafkaRecord.toSyslogRecord().getPayload().toString());
                counter++;
            }

            kafkaRecord = messages.get(0).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":1, \"offset\":" + 14 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            Assertions.assertEquals(0, kafkaRecord.size());
            counter++;

            kafkaRecord = messages.get(0).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":1, \"offset\":" + 15 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            KafkaRecordImpl finalKafkaRecord3 = kafkaRecord;
            e = Assertions.assertThrows(ParseException.class, finalKafkaRecord3::toSyslogRecord);
            Assertions.assertEquals("PRIORITY < missing", e.getMessage());
            counter++;

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                kafkaRecord = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":9, \"offset\":" + i + "}",
                                kafkaRecord.offsetToJSON()
                        );
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), kafkaRecord.toSyslogRecord().getPayload().toString());
                counter++;
            }

            kafkaRecord = messages.get(0).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":9, \"offset\":" + 14 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            Assertions.assertEquals(0, kafkaRecord.size());
            counter++;

            kafkaRecord = messages.get(0).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":9, \"offset\":" + 15 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            KafkaRecordImpl finalKafkaRecord4 = kafkaRecord;
            e = Assertions.assertThrows(ParseException.class, finalKafkaRecord4::toSyslogRecord);
            Assertions.assertEquals("PRIORITY < missing", e.getMessage());
            counter++;

            Assertions.assertEquals(80, counter);

            counter = 0;
            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                kafkaRecord = messages.get(1).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":8, \"offset\":" + i + "}",
                                kafkaRecord.offsetToJSON()
                        );
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), kafkaRecord.toSyslogRecord().getPayload().toString());
                counter++;
            }

            kafkaRecord = messages.get(1).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":8, \"offset\":" + 14 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            Assertions.assertEquals(0, kafkaRecord.size());
            counter++;

            kafkaRecord = messages.get(1).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":8, \"offset\":" + 15 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            KafkaRecordImpl finalKafkaRecord5 = kafkaRecord;
            e = Assertions.assertThrows(ParseException.class, finalKafkaRecord5::toSyslogRecord);
            Assertions.assertEquals("PRIORITY < missing", e.getMessage());
            counter++;

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                kafkaRecord = messages.get(1).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":6, \"offset\":" + i + "}",
                                kafkaRecord.offsetToJSON()
                        );
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), kafkaRecord.toSyslogRecord().getPayload().toString());
                counter++;
            }

            kafkaRecord = messages.get(1).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":6, \"offset\":" + 14 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            Assertions.assertEquals(0, kafkaRecord.size());
            counter++;

            kafkaRecord = messages.get(1).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":6, \"offset\":" + 15 + "}",
                            kafkaRecord.offsetToJSON()
                    );

            KafkaRecordImpl finalKafkaRecord6 = kafkaRecord;
            e = Assertions.assertThrows(ParseException.class, finalKafkaRecord6::toSyslogRecord);
            Assertions.assertEquals("PRIORITY < missing", e.getMessage());
            counter++;

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                kafkaRecord = messages.get(1).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":4, \"offset\":" + i + "}",
                                kafkaRecord.offsetToJSON()
                        );
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), kafkaRecord.toSyslogRecord().getPayload().toString());
                counter++;
            }

            kafkaRecord = messages.get(1).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":4, \"offset\":" + 14 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            Assertions.assertEquals(0, kafkaRecord.size());
            counter++;

            kafkaRecord = messages.get(1).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":4, \"offset\":" + 15 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            KafkaRecordImpl finalKafkaRecord7 = kafkaRecord;
            e = Assertions.assertThrows(ParseException.class, finalKafkaRecord7::toSyslogRecord);
            Assertions.assertEquals("PRIORITY < missing", e.getMessage());
            counter++;

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                kafkaRecord = messages.get(1).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":2, \"offset\":" + i + "}",
                                kafkaRecord.offsetToJSON()
                        );
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), kafkaRecord.toSyslogRecord().getPayload().toString());
                counter++;
            }

            kafkaRecord = messages.get(1).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":2, \"offset\":" + 14 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            Assertions.assertEquals(0, kafkaRecord.size());
            counter++;

            kafkaRecord = messages.get(1).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":2, \"offset\":" + 15 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            KafkaRecordImpl finalKafkaRecord8 = kafkaRecord;
            e = Assertions.assertThrows(ParseException.class, finalKafkaRecord8::toSyslogRecord);
            Assertions.assertEquals("PRIORITY < missing", e.getMessage());
            counter++;

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                kafkaRecord = messages.get(1).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":" + i + "}",
                                kafkaRecord.offsetToJSON()
                        );
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), kafkaRecord.toSyslogRecord().getPayload().toString());
                counter++;
            }

            kafkaRecord = messages.get(1).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":" + 14 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            Assertions.assertEquals(0, kafkaRecord.size());
            counter++;

            kafkaRecord = messages.get(1).get(counter);
            Assertions
                    .assertEquals(
                            "{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":" + 15 + "}",
                            kafkaRecord.offsetToJSON()
                    );
            KafkaRecordImpl finalKafkaRecord9 = kafkaRecord;
            e = Assertions.assertThrows(ParseException.class, finalKafkaRecord9::toSyslogRecord);
            Assertions.assertEquals("PRIORITY < missing", e.getMessage());
            counter++;

            Assertions.assertEquals(80, counter);

        });
    }

    @Disabled(value = "This needs refactoring")
    @Test
    public void readCoordinatorTest1Thread() {

        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            Config config = new Config();
            Map<TopicPartition, Long> hdfsStartOffsets = new HashMap<>();
            ArrayList<List<KafkaRecordImpl>> messages = new ArrayList<>();
            Consumer<List<KafkaRecordImpl>> output = message -> messages.add(message);

            ReadCoordinator readCoordinator = new ReadCoordinator(
                    "testConsumerTopic",
                    config.getKafkaConsumerProperties(),
                    (BatchDistributionImpl) output, // FIXME: Dont/Can't use casting like this.
                    hdfsStartOffsets
            );
            Thread readThread = new Thread(null, readCoordinator, "testConsumerTopic0"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(10000);
            Assertions.assertEquals(1, messages.size());
            Assertions.assertEquals(160, messages.get(0).size()); // Assert that expected amount of records has been consumed by the consumer.

            // Assert that all the record contents are correct, every topic partition has identical set of offset-message pairings.
            List<String> list = new ArrayList<String>();
            list.add("[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!");
            list.add("[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!");
            list.add("470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.");
            list
                    .add(
                            "470646  [Thread-3] INFO  com.teragrep@Disabled(value = \"This needs refactoring\").jla_02.Logback Audit - Logback-audit says hi."
                    );
            list.add("470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.");
            list
                    .add(
                            "25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]"
                    );

            KafkaRecordImpl recordOffset;
            Iterator<String> iterator;
            List<Integer> partitionList = new ArrayList<Integer>();
            partitionList.add(7);
            partitionList.add(8);
            partitionList.add(5);
            partitionList.add(6);
            partitionList.add(3);
            partitionList.add(4);
            partitionList.add(1);
            partitionList.add(2);
            partitionList.add(0);
            partitionList.add(9);
            int counter = 0;
            for (int partition : partitionList) {
                iterator = list.iterator();
                for (int i = 0; i <= 13; i++) {
                    recordOffset = messages.get(0).get(counter);
                    Assertions
                            .assertEquals(
                                    "{\"topic\":\"testConsumerTopic\", \"partition\":" + partition + ", \"offset\":" + i
                                            + "}",
                                    recordOffset.offsetToJSON()
                            );
                    Assertions.assertTrue(iterator.hasNext());
                    Assertions.assertEquals(iterator.next(), recordOffset.toSyslogRecord().getPayload().toString());
                    counter++;
                }

                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":" + partition + ", \"offset\":" + 14
                                        + "}",
                                recordOffset.offsetToJSON()
                        );
                Assertions.assertEquals(0, recordOffset.size());
                counter++;

                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":" + partition + ", \"offset\":" + 15
                                        + "}",
                                recordOffset.offsetToJSON()
                        );
                ParseException e = Assertions.assertThrows(ParseException.class, recordOffset::toSyslogRecord);
                Assertions.assertEquals("PRIORITY < missing", e.getMessage());
                counter++;
            }

            Assertions.assertEquals(160, counter); // All 160 records were asserted.

        });
    }

}
