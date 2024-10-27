/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp
 *
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of DBeaver Corp and its suppliers, if any.
 * The intellectual and technical concepts contained
 * herein are proprietary to DBeaver Corp and its suppliers
 * and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from DBeaver Corp.
 */
package com.dbeaver.jdbc.driver.libsql;


import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;

public class LSqlDriver implements Driver {

    static final java.util.logging.Logger parentLogger = java.util.logging.Logger.getLogger("com.dbeaver.jdbc.upd.driver.driver");

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Matcher matcher = LSqlConstants.CONNECTION_URL_PATTERN.matcher(url);
        if (!matcher.matches()) {
            throw new LSqlException(
                "Invalid connection URL: " + url +
                ".\nExpected URL format: " + LSqlConstants.CONNECTION_URL_EXAMPLE);
        }

        throw new SQLFeatureNotSupportedException();
/*
        UPDEndpoint endpoint = new UPDEndpoint();
        endpoint.setServerAddress(matcher.group(1));
        endpoint.setServerPort(CommonUtils.toInt(matcher.group(2), endpoint.getServerPort()));
        endpoint.setProjectId(matcher.group(3));
        String dsRef = matcher.group(4);
        if (!dsRef.contains("=")) {
            endpoint.setDataSourceId(dsRef);
        } else {

        }

        Map<String, String> serverProperties = new LinkedHashMap<>();
        Map<String, String> driverProperties = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> pe : info.entrySet()) {
            String propName = pe.getKey().toString();
            if (propName.startsWith(UPDConstants.DRIVER_PROPERTY_DBEAVER)) {
                serverProperties.put(
                    propName.substring(UPDConstants.DRIVER_PROPERTY_DBEAVER.length()),
                    (String)pe.getValue());
            } else {
                driverProperties.put(propName, (String)pe.getValue());
            }
        }

        UPDProtocol protocol = openServerConnection(endpoint, serverProperties);
        try {
            return new LSqlConnection(this, protocol, endpoint, serverProperties, driverProperties);
        } catch (Throwable e) {
            // Terminate remote client
            protocol.close();
            throw e;
        }
*/
    }

/*
    private UPDProtocol openServerConnection(
        @NotNull UPDEndpoint endpoint,
        @NotNull Map<String, String> serverProperties
    ) throws SQLException {
        StringBuilder url = new StringBuilder();
        String schema = endpoint.getServerPort() == 0 ||
            endpoint.getServerPort() == UPDConstants.DEFAULT_SERVER_PORT ? "https" : "http";
        url.append(schema).append("://");
        url.append(endpoint.getServerAddress());
        if (endpoint.getServerPort() > 0) {
            url.append(":").append(endpoint.getServerPort());
        }
        url.append("/api/upd/?");
        if (endpoint.getProjectId() != null) {
            url.append(UPDConstants.SERVER_URL_PARAM_PROJECT).append("=").append(endpoint.getProjectId());
        } else {
            throw new LSqlException("Datasource ID is missing");
        }
        if (endpoint.getDataSourceId() != null) {
            url.append(UPDConstants.SERVER_URL_PARAM_DATASOURCE_ID).append("=").append(endpoint.getDataSourceId());
        } else {
            throw new LSqlException("Datasource ID is missing");
        }

        URI serverURI;
        try {
            serverURI = new URI(url.toString());
        } catch (URISyntaxException e) {
            throw new LSqlException("Invalid server URI [" + url + "]", e);
        }

        return JsonRpcClient
            .builder(serverURI, UPDProtocol.class)
            .setUserAgent(getDriverName() + " " + getFullVersion())
            .create();
    }
*/

    @Override
    public boolean acceptsURL(String url) {
        return LSqlConstants.CONNECTION_URL_PATTERN.matcher(url).matches();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[] {

        };
    }

    @Override
    public int getMajorVersion() {
        return LSqlConstants.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion() {
        return LSqlConstants.DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return parentLogger;
    }

    public String getDriverName() {
        return LSqlConstants.DRIVER_NAME;
    }

    public String getFullVersion() {
        return getMajorVersion() + "." + getMinorVersion() + " (" + LSqlConstants.DRIVER_INFO + ")";
    }
}
