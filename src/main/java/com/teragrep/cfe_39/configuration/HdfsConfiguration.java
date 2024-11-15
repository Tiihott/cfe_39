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
package com.teragrep.cfe_39.configuration;

import org.apache.logging.log4j.core.config.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class HdfsConfiguration {

    private final Logger LOGGER = LoggerFactory.getLogger(HdfsConfiguration.class);

    private final Map<String, String> config;

    public HdfsConfiguration(Map<String, String> config) {
        this.config = config;
    }

    // printers for the configuration parameters.

    public long pruneOffset() {
        final String numString = config.get("pruneOffset");
        if (numString == null) {
            throw new ConfigurationException("Configuration error. <pruneOffset> must be set.");
        }
        else {
            final long pruneOffset;
            try {
                pruneOffset = Long.parseLong(numString);
            }
            catch (NumberFormatException e) {
                LOGGER.error("Configuration error. Invalid value for <pruneOffset>: <{}>", e.getMessage());
                throw new RuntimeException(e);
            }
            if (pruneOffset <= 0) {
                throw new ConfigurationException("Configuration error. <pruneOffset> must be a positive integer.");
            }
            else {
                return pruneOffset;
            }
        }
    }

    public String hdfsUri() {
        final String hdfsUri = config.get("hdfsuri");
        if (hdfsUri == null) {
            throw new ConfigurationException("Configuration error. <hdfsuri> must be set.");
        }
        else {
            return hdfsUri;
        }
    }

    public String hdfsPath() {
        final String hdfsPath = config.get("hdfsPath");
        if (hdfsPath == null) {
            throw new ConfigurationException("Configuration error. <hdfsPath> must be set.");
        }
        else {
            return hdfsPath;
        }
    }

    public String javaSecurityKrb5Kdc() {
        final String javaSecurityKrb5Kdc = config.get("java.security.krb5.kdc");
        if (javaSecurityKrb5Kdc == null) {
            throw new ConfigurationException("Configuration error. <java.security.krb5.kdc> must be set.");
        }
        else {
            return javaSecurityKrb5Kdc;
        }
    }

    public String javaSecurityKrb5Realm() {
        final String javaSecurityKrb5Realm = config.get("java.security.krb5.realm");
        if (javaSecurityKrb5Realm == null) {
            throw new ConfigurationException("Configuration error. <java.security.krb5.realm> must be set.");
        }
        else {
            return javaSecurityKrb5Realm;
        }
    }

    public String hadoopSecurityAuthentication() {
        final String hadoopSecurityAuthentication = config.get("hadoop.security.authentication");
        if (hadoopSecurityAuthentication == null) {
            throw new ConfigurationException("Configuration error. <hadoop.security.authentication> must be set.");
        }
        else {
            return hadoopSecurityAuthentication;
        }
    }

    public String hadoopSecurityAuthorization() {
        final String hadoopSecurityAuthorization = config.get("hadoop.security.authorization");
        if (hadoopSecurityAuthorization == null) {
            throw new ConfigurationException("Configuration error. <hadoop.security.authorization> must be set.");
        }
        else {
            return hadoopSecurityAuthorization;
        }
    }

    public String dfsNamenodeKerberosPrincipalPattern() {
        final String dfsNamenodeKerberosPrincipalPattern = config.get("dfs.namenode.kerberos.principal.pattern");
        if (dfsNamenodeKerberosPrincipalPattern == null) {
            throw new ConfigurationException(
                    "Configuration error. <dfs.namenode.kerberos.principal.pattern> must be set."
            );
        }
        else {
            return dfsNamenodeKerberosPrincipalPattern;
        }
    }

    public String KerberosKeytabUser() {
        final String KerberosKeytabUser = config.get("KerberosKeytabUser");
        if (KerberosKeytabUser == null) {
            throw new ConfigurationException("Configuration error. <KerberosKeytabUser> must be set.");
        }
        else {
            return KerberosKeytabUser;
        }
    }

    public String KerberosKeytabPath() {
        final String KerberosKeytabPath = config.get("KerberosKeytabPath");
        if (KerberosKeytabPath == null) {
            throw new ConfigurationException("Configuration error. <KerberosKeytabPath> must be set.");
        }
        else {
            return KerberosKeytabPath;
        }
    }

    public String dfsClientUseDatanodeHostname() {
        final String dfsClientUseDatanodeHostname = config.get("dfs.client.use.datanode.hostname");
        if (dfsClientUseDatanodeHostname == null) {
            throw new ConfigurationException("Configuration error. <dfs.client.use.datanode.hostname> must be set.");
        }
        else {
            return dfsClientUseDatanodeHostname;
        }
    }

    public String hadoopKerberosKeytabLoginAutorenewalEnabled() {
        final String hadoopKerberosKeytabLoginAutorenewalEnabled = config
                .get("hadoop.kerberos.keytab.login.autorenewal.enabled");
        if (hadoopKerberosKeytabLoginAutorenewalEnabled == null) {
            throw new ConfigurationException(
                    "Configuration error. <hadoop.kerberos.keytab.login.autorenewal.enabled> must be set."
            );
        }
        else {
            return hadoopKerberosKeytabLoginAutorenewalEnabled;
        }
    }

    public String dfsDataTransferProtection() {
        final String dfsDataTransferProtection = config.get("dfs.data.transfer.protection");
        if (dfsDataTransferProtection == null) {
            throw new ConfigurationException("Configuration error. <dfs.data.transfer.protection> must be set.");
        }
        else {
            return dfsDataTransferProtection;
        }
    }

    public String dfsEncryptDataTransferCipherSuites() {
        final String dfsEncryptDataTransferCipherSuites = config.get("dfs.encrypt.data.transfer.cipher.suites");
        if (dfsEncryptDataTransferCipherSuites == null) {
            throw new ConfigurationException(
                    "Configuration error. <dfs.encrypt.data.transfer.cipher.suites> must be set."
            );
        }
        else {
            return dfsEncryptDataTransferCipherSuites;
        }
    }

}
