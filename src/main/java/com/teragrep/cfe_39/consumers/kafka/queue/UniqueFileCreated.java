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
package com.teragrep.cfe_39.consumers.kafka.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.BiPredicate;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

// UniqueFileCreated responsibility is to create a new File object that doesn't interfere with any existing files on the given directory.
public class UniqueFileCreated {

    private static final Logger LOGGER = LoggerFactory.getLogger(UniqueFileCreated.class);

    private final Path queueDirectory;
    private final String queueNamePrefix;

    public UniqueFileCreated(String queueDirectory, String queueNamePrefix) {
        this.queueDirectory = Paths.get(queueDirectory);
        this.queueNamePrefix = queueNamePrefix;
        if (!Files.isDirectory(this.queueDirectory)) {
            throw new IllegalArgumentException("Provided path is not a directory <[" + queueDirectory + "]>");
        }
        if (!Files.isWritable(this.queueDirectory)) {
            throw new IllegalArgumentException("Provided path is not writeable <[" + queueDirectory + "]>");
        }
    }

    private File getNextWritableFilename() throws IOException {

        try (
                Stream<Path> files = Files.find(queueDirectory, 1, getFileMatcher(queueNamePrefix), FileVisitOption.FOLLOW_LINKS)
        ) {

            long sequenceNumber = files.mapToLong(getPathToSequenceNumberFunction()).max().orElse(0);

            long nextSequenceNumber = sequenceNumber + 1;

            // create next
            return new File(
                    queueDirectory.toAbsolutePath() + File.separator + queueNamePrefix + "." + nextSequenceNumber
            );
        }
        catch (UncheckedIOException uncheckedIOException) {
            // just retry, reader modified the directory
            return getNextWritableFilename();
        }
    }

    public File getNextWritableFile() throws IOException {
        if (queueNamePrefix.isEmpty()) {
            throw new IOException("No queueNamePrefix set");
        }
        else {
            return getNextWritableFilename();
        }
    }

    private BiPredicate<Path, BasicFileAttributes> getFileMatcher(String queueNamePrefixInput) {
        return (path, basicFileAttributes) -> {
            if (!path.getFileName().toString().startsWith(queueNamePrefixInput)) {
                return false;
            }
            else if (path.getFileName().toString().endsWith(".state")) {
                return false;
            }
            else if (!basicFileAttributes.isRegularFile()) {
                return false;
            }
            else {
                LOGGER.trace("getFileMatcher returning: <{}>", path);
                return true;
            }
        };
    }

    private ToLongFunction<Path> getPathToSequenceNumberFunction() {
        return path -> {
            String pathString = path.toString();

            int dotPosition = pathString.lastIndexOf('.');

            String sequenceNumberString = pathString.substring(dotPosition + 1);

            return Long.parseLong(sequenceNumberString);
        };
    }
}
