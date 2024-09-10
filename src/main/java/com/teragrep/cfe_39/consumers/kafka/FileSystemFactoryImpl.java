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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;

public final class FileSystemFactoryImpl implements FileSystemFactory {

    private final HdfsConfiguration conf;
    private final ConfigurationImpl configuration;

    public FileSystemFactoryImpl(ConfigurationImpl configuration) {
        this.conf = new HdfsConfiguration();
        this.configuration = configuration;
    }

    public FileSystem create(boolean initializeUGI) throws IOException {
        FileSystem fs;
        if ("kerberos".equals(configuration.valueOf("hadoop.security.authentication"))) {
            // Initializing the FileSystem with kerberos.
            String hdfsuri = configuration.valueOf("hdfsuri"); // Get from config.
            // set kerberos host and realm
            System.setProperty("java.security.krb5.realm", configuration.valueOf("java.security.krb5.realm"));
            System.setProperty("java.security.krb5.kdc", configuration.valueOf("java.security.krb5.kdc"));
            conf.clear();
            // enable kerberus
            conf.set("hadoop.security.authentication", configuration.valueOf("hadoop.security.authentication"));
            conf.set("hadoop.security.authorization", configuration.valueOf("hadoop.security.authorization"));
            conf
                    .set(
                            "hadoop.kerberos.keytab.login.autorenewal.enabled",
                            configuration.valueOf("hadoop.kerberos.keytab.login.autorenewal.enabled")
                    );
            conf.set("fs.defaultFS", hdfsuri); // Set FileSystem URI
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName()); // Maven stuff?
            conf.set("fs.file.impl", LocalFileSystem.class.getName()); // Maven stuff?
            /* hack for running locally with fake DNS records
             set this to true if overriding the host name in /etc/hosts*/
            conf.set("dfs.client.use.datanode.hostname", configuration.valueOf("dfs.client.use.datanode.hostname"));
            /* server principal
             the kerberos principle that the namenode is using*/
            conf
                    .set(
                            "dfs.namenode.kerberos.principal.pattern",
                            configuration.valueOf("dfs.namenode.kerberos.principal.pattern")
                    );
            // set sasl
            conf.set("dfs.data.transfer.protection", configuration.valueOf("dfs.data.transfer.protection"));
            conf
                    .set(
                            "dfs.encrypt.data.transfer.cipher.suites",
                            configuration.valueOf("dfs.encrypt.data.transfer.cipher.suites")
                    );
            if (initializeUGI) {
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation
                        .loginUserFromKeytab(configuration.valueOf("KerberosKeytabUser"), configuration.valueOf("KerberosKeytabPath"));
            }
            // filesystem for HDFS access is set here
            fs = FileSystem.get(conf);
        }
        else {
            // Initializing the FileSystem with minicluster.
            String hdfsuri = configuration.valueOf("hdfsuri");
            // ====== Init HDFS File System Object
            conf.clear();
            // Set FileSystem URI
            conf.set("fs.defaultFS", hdfsuri);
            // Because of Maven
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            // Set HADOOP user
            System.setProperty("HADOOP_USER_NAME", "hdfs");
            System.setProperty("hadoop.home.dir", "/");
            //Get the filesystem - HDFS
            fs = FileSystem.get(URI.create(hdfsuri), conf);
        }
        return fs;
    }

}
