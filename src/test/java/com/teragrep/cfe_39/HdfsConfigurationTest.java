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

import com.teragrep.cfe_39.configuration.HdfsConfiguration;
import com.teragrep.cnf_01.PathConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class HdfsConfigurationTest {

    private final Logger LOGGER = LoggerFactory.getLogger(HdfsConfigurationTest.class);

    @Test
    public void configurationTest() {
        assertDoesNotThrow(() -> {
            final PathConfiguration hdfsPathConfiguration = new PathConfiguration(
                    System.getProperty("user.dir") + "/src/test/resources/valid.hdfs.properties"
            );
            final Map<String, String> hdfsMap;
            hdfsMap = hdfsPathConfiguration.asMap();
            Assertions
                    .assertEquals(
                            "{pruneOffset=157784760000, hdfsuri=hdfs://localhost:45937/, dfs.namenode.kerberos.principal.pattern=test, hadoop.security.authentication=kerberos, dfs.encrypt.data.transfer.cipher.suites=test, java.security.krb5.kdc=test, KerberosKeytabPath=test, dfs.data.transfer.protection=test, dfs.client.use.datanode.hostname=false, hadoop.kerberos.keytab.login.autorenewal.enabled=true, KerberosKeytabUser=test, java.security.krb5.realm=test, hadoop.security.authorization=test, hdfsPath=hdfs:///opt/teragrep/cfe_39/srv/}",
                            hdfsMap.toString()
                    );
            HdfsConfiguration hdfsConfig = new HdfsConfiguration(hdfsMap);

            // Assert that printers return correct values.
            Assertions.assertEquals(157784760000L, hdfsConfig.pruneOffset());
            Assertions.assertEquals("hdfs://localhost:45937/", hdfsConfig.hdfsUri());
            Assertions.assertEquals("hdfs:///opt/teragrep/cfe_39/srv/", hdfsConfig.hdfsPath());
            Assertions.assertEquals("test", hdfsConfig.javaSecurityKrb5Kdc());
            Assertions.assertEquals("test", hdfsConfig.javaSecurityKrb5Realm());
            Assertions.assertEquals("kerberos", hdfsConfig.hadoopSecurityAuthentication());
            Assertions.assertEquals("test", hdfsConfig.hadoopSecurityAuthorization());
            Assertions.assertEquals("test", hdfsConfig.dfsNamenodeKerberosPrincipalPattern());
            Assertions.assertEquals("test", hdfsConfig.KerberosKeytabUser());
            Assertions.assertEquals("test", hdfsConfig.KerberosKeytabPath());
            Assertions.assertEquals("false", hdfsConfig.dfsClientUseDatanodeHostname());
            Assertions.assertEquals("true", hdfsConfig.hadoopKerberosKeytabLoginAutorenewalEnabled());
            Assertions.assertEquals("test", hdfsConfig.dfsDataTransferProtection());
            Assertions.assertEquals("test", hdfsConfig.dfsEncryptDataTransferCipherSuites());
        });
    }
}
