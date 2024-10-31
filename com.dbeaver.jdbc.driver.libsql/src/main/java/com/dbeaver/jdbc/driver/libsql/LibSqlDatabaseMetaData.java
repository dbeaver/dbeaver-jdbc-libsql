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

import com.dbeaver.jdbc.model.AbstractJdbcDatabaseMetaData;
import org.jkiss.code.NotNull;
import org.jkiss.utils.CommonUtils;
import org.jkiss.utils.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LibSqlDatabaseMetaData extends AbstractJdbcDatabaseMetaData<LibSqlConnection> {

    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\w+)\\s+([0-9.]+)\\s+(.+)");

    private String serverVersion;

    public LibSqlDatabaseMetaData(@NotNull LibSqlConnection connection) {
        super(connection, connection.getUrl());
    }

    private void readServerVersion() throws SQLException {
        if (serverVersion != null) {
            return;
        }
        try {
            HttpURLConnection con = connection.getClient().openConnection("version");
            try (InputStream is = con.getInputStream()) {
                serverVersion = IOUtils.readLine(is);
            }
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        readServerVersion();
        return serverVersion;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        readServerVersion();
        Matcher matcher = VERSION_PATTERN.matcher(serverVersion);
        if (matcher.matches()) {
            return matcher.group(2);
        }
        return serverVersion;
    }

    @Override
    public String getDriverName() {
        return connection.getDriver().getDriverName();
    }

    @Override
    public String getDriverVersion() {
        return connection.getDriver().getFullVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return connection.getDriver().getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return connection.getDriver().getMinorVersion();
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        verifySchemaParameters(catalog, schemaPattern);
        try (PreparedStatement dbStat = connection.prepareStatement(
            "SELECT NULL as TABLE_CAT, NULL AS TABLE_SCHEM," +
                "name AS TABLE_NAME,type as TABLE_TYPE, " +
                "NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME " +
                "FROM sqlite_master WHERE type='table'")
        ) {
            return dbStat.executeQuery();
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableName, String columnNamePattern) throws SQLException {
        verifySchemaParameters(catalog, schemaPattern);
        try (PreparedStatement dbStat = connection.prepareStatement(
            "SELECT NULL as TABLE_CAT, NULL AS TABLE_SCHEM,'" + tableName + "' AS TABLE_NAME," +
                "name as COLUMN_NAME," + Types.VARCHAR + " AS DATA_TYPE, type AS TYPE_NAME, 0 AS COLUMN_SIZE," +
                "NULL AS REMARKS,cid AS ORDINAL_POSITION " +
                "FROM PRAGMA_TABLE_INFO('" + tableName + "')")
        ) {
            return dbStat.executeQuery();
        }
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(connection, table);
        String[] columns = pkFinder.getColumns();

        StringBuilder sql = new StringBuilder();
        sql.append("select null as TABLE_CAT, null as TABLE_SCHEM, '")
            .append(escape(table))
            .append("' as TABLE_NAME, cn as COLUMN_NAME, ks as KEY_SEQ, pk as PK_NAME from (");

        if (columns == null) {
            sql.append("select null as cn, null as pk, 0 as ks) limit 0;");
            return executeQuery(sql.toString());
        }

        String pkName = pkFinder.getName();
        if (pkName != null) {
            pkName = "'" + pkName + "'";
        }

        for (int i = 0; i < columns.length; i++) {
            if (i > 0) sql.append(" union ");
            sql.append("select ")
                .append(pkName)
                .append(" as pk, '")
                .append(escape(unquoteIdentifier(columns[i])))
                .append("' as cn, ")
                .append(i + 1)
                .append(" as ks");
        }

        sql.append(") order by cn;");
        return executeQuery(sql.toString());
    }

    private static class IndexInfo {
        String indexName;
        int indexId;

        public IndexInfo(String indexName, int indexId) {
            this.indexName = indexName;
            this.indexId = indexId;
        }
    }
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        {
            StringBuilder sql = new StringBuilder();

            // define the column header
            // this is from the JDBC spec, it is part of the driver protocol
            sql.append("select null as TABLE_CAT, null as TABLE_SCHEM, '")
                .append(escape(table))
                .append("' as TABLE_NAME, un as NON_UNIQUE, null as INDEX_QUALIFIER, n as INDEX_NAME, ")
                .append(Integer.toString(DatabaseMetaData.tableIndexOther)).append(" as TYPE, op as ORDINAL_POSITION, ")
                .append("cn as COLUMN_NAME, null as ASC_OR_DESC, 0 as CARDINALITY, 0 as PAGES, null as FILTER_CONDITION from (");

            // this always returns a result set now, previously threw exception
            List<IndexInfo> indexList = new ArrayList<>();
            try (ResultSet rs = executeQuery("pragma index_list('" + escape(table) + "')")) {
                while (rs.next()) {
                    IndexInfo indexInfo = new IndexInfo(
                        rs.getString(2),
                        rs.getInt(3)
                    );
                    indexList.add(indexInfo);
                }
            }
            if (indexList.isEmpty()) {
                // if pragma index_list() returns no information, use this null block
                sql.append("select null as un, null as n, null as op, null as cn) limit 0;");
                return executeQuery(sql.toString());
            } else {
                // loop over results from pragma call, getting specific info for each index
                List<String> unionAll = new ArrayList<>();
                for (IndexInfo currentIndex : indexList) {
                    String indexName = currentIndex.indexName;
                    try (ResultSet rs = executeQuery("pragma index_info('" + escape(indexName) + "')")) {
                        while (rs.next()) {
                            StringBuilder sqlRow = new StringBuilder();

                            String colName = rs.getString(3);
                            sqlRow.append("select ")
                                .append(1 - currentIndex.indexId).append(" as un,'")
                                .append(escape(indexName)).append("' as n,")
                                .append(rs.getInt(1) + 1).append(" as op,");
                            if (colName == null) { // expression index
                                sqlRow.append("null");
                            } else {
                                sqlRow.append("'").append(escape(colName)).append("'");
                            }
                            sqlRow.append(" as cn");

                            unionAll.add(sqlRow.toString());
                        }
                    }
                }

                String sqlBlock = String.join(" union all ", unionAll);
                sql.append(sqlBlock).append(");");
                return executeQuery(sql.toString());
            }
        }
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        {
            StringBuilder sql = new StringBuilder();

            sql.append("select ")
                .append(quote(catalog)).append(" as PKTABLE_CAT, ")
                .append(quote(schema)).append(" as PKTABLE_SCHEM, ")
                .append("ptn as PKTABLE_NAME, pcn as PKCOLUMN_NAME, ")
                .append(quote(catalog)).append(" as FKTABLE_CAT, ")
                .append(quote(schema)).append(" as FKTABLE_SCHEM, ")
                .append(quote(table)).append(" as FKTABLE_NAME, ")
                .append("fcn as FKCOLUMN_NAME, ks as KEY_SEQ, ur as UPDATE_RULE, dr as DELETE_RULE, fkn as FK_NAME, pkn as PK_NAME, ")
                .append(DatabaseMetaData.importedKeyInitiallyDeferred)
                .append(" as DEFERRABILITY from (");

            // Use a try catch block to avoid "query does not return ResultSet" error
            try (ResultSet rs = executeQuery("pragma foreign_key_list('" + escape(table) + "')")) {

                final ImportedKeyFinder impFkFinder = new ImportedKeyFinder(connection, table);
                List<ImportedKeyFinder.ForeignKey> fkNames = impFkFinder.getFkList();

                int i = 0;
                for (; rs.next(); i++) {
                    int keySeq = rs.getInt(2) + 1;
                    int keyId = rs.getInt(1);
                    String PKTabName = rs.getString(3);
                    String FKColName = rs.getString(4);
                    String PKColName = rs.getString(5);

                    String pkName = null;
                    try {
                        PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(connection, PKTabName);
                        pkName = pkFinder.getName();
                        if (PKColName == null) {
                            PKColName = pkFinder.getColumns()[0];
                        }
                    } catch (SQLException ignored) {
                    }

                    String updateRule = rs.getString(6);
                    String deleteRule = rs.getString(7);

                    if (i > 0) {
                        sql.append(" union all ");
                    }

                    String fkName = null;
                    if (fkNames.size() > keyId) fkName = fkNames.get(keyId).getFkName();

                    sql.append("select ")
                        .append(keySeq).append(" as ks,")
                        .append("'")
                        .append(escape(PKTabName)).append("' as ptn, '")
                        .append(escape(FKColName)).append("' as fcn, '")
                        .append(escape(PKColName)).append("' as pcn,")
                        .append("case '")
                        .append(escape(updateRule))
                        .append("'")
                        .append(" when 'NO ACTION' then ").append(DatabaseMetaData.importedKeyNoAction)
                        .append(" when 'CASCADE' then ").append(DatabaseMetaData.importedKeyCascade)
                        .append(" when 'RESTRICT' then ").append(DatabaseMetaData.importedKeyRestrict)
                        .append(" when 'SET NULL' then ").append(DatabaseMetaData.importedKeySetNull)
                        .append(" when 'SET DEFAULT' then ").append(DatabaseMetaData.importedKeySetDefault)
                        .append(" end as ur, ")
                        .append("case '")
                        .append(escape(deleteRule))
                        .append("'")
                        .append(" when 'NO ACTION' then ").append(DatabaseMetaData.importedKeyNoAction)
                        .append(" when 'CASCADE' then ").append(DatabaseMetaData.importedKeyCascade)
                        .append(" when 'RESTRICT' then ").append(DatabaseMetaData.importedKeyRestrict)
                        .append(" when 'SET NULL' then ").append(DatabaseMetaData.importedKeySetNull)
                        .append(" when 'SET DEFAULT' then ").append(DatabaseMetaData.importedKeySetDefault)
                        .append(" end as dr, ")
                        .append(fkName == null ? "''" : quote(fkName)).append(" as fkn, ")
                        .append(pkName == null ? "''" : quote(pkName)).append(" as pkn");
                }
                if (i == 0) {
                    sql.append("select -1 as ks, '' as ptn, '' as fcn, '' as pcn, ")
                        .append(DatabaseMetaData.importedKeyNoAction).append(" as ur, ")
                        .append(DatabaseMetaData.importedKeyNoAction).append(" as dr, ")
                        .append(" '' as fkn, ")
                        .append(" '' as pkn ")
                        .append(") limit 0;");
                } else {
                    sql.append(") ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ");
                }
            }
            return executeQuery(sql.toString());
        }
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return super.getExportedKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        if (parentTable == null) {
            return getExportedKeys(parentCatalog, parentSchema, parentTable);
        }
        if (foreignTable == null) {
            return getImportedKeys(parentCatalog, parentSchema, parentTable);
        }

        String query =
            "select " + quote(parentCatalog)
            + " as PKTABLE_CAT, " + quote(parentSchema)
            + " as PKTABLE_SCHEM, " + quote(parentTable)
            + " as PKTABLE_NAME, "
            + "'' as PKCOLUMN_NAME, " + quote(foreignCatalog)
            + " as FKTABLE_CAT, " + quote(foreignSchema)
            + " as FKTABLE_SCHEM, " + quote(foreignTable)
            + " as FKTABLE_NAME, "
            + "'' as FKCOLUMN_NAME, -1 as KEY_SEQ, 3 as UPDATE_RULE, 3 as DELETE_RULE, '' as FK_NAME, '' as PK_NAME, "
            + DatabaseMetaData.importedKeyInitiallyDeferred + " as DEFERRABILITY limit 0 ";
        return executeQuery(query);
    }

    private static void verifySchemaParameters(String catalog, String schemaPattern) throws SQLException {
        if (!CommonUtils.isEmpty(catalog)) {
            throw new SQLException("Catalogs are not supported");
        }
        if (!CommonUtils.isEmpty(schemaPattern)) {
            throw new SQLException("Schemas are not supported");
        }
    }

    public static String quote(String identifier) {
        return "'" + identifier + "'";
    }

    /**
     * Follow rules in <a href="https://www.sqlite.org/lang_keywords.html">SQLite Keywords</a>
     *
     * @param name Identifier name
     * @return Unquoted identifier
     */
    public static String unquoteIdentifier(String name) {
        if (name == null) return name;
        name = name.trim();
        if (name.length() > 2
            && ((name.startsWith("`") && name.endsWith("`"))
                || (name.startsWith("\"") && name.endsWith("\""))
                || (name.startsWith("[") && name.endsWith("]")))) {
            // unquote to be consistent with column names returned by getColumns()
            name = name.substring(1, name.length() - 1);
        }
        return name;
    }

    public static String escape(final String val) {
        if (val.indexOf('\'') == 1) {
            return val;
        }
        int len = val.length();
        StringBuilder buf = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            if (val.charAt(i) == '\'') {
                buf.append('\'');
            }
            buf.append(val.charAt(i));
        }
        return buf.toString();
    }

    private ResultSet executeQuery(String query) throws SQLException {
        try (Statement stat = connection.createStatement()) {
            return stat.executeQuery(query);
        }
    }

    public static ResultSet executeQuery(Connection connection, String query) throws SQLException {
        try (Statement stat = connection.createStatement()) {
            return stat.executeQuery(query);
        }
    }

    /**
     * Parses the sqlite_schema table for a table's primary key
     * Original algorithm taken from Xerial SQLite driver.
     * */
    static class PrimaryKeyFinder {
        /** Pattern used to extract column order for an unnamed primary key. */
        protected static final Pattern PK_UNNAMED_PATTERN =
            Pattern.compile(
                ".*PRIMARY\\s+KEY\\s*\\((.*?)\\).*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

        /** Pattern used to extract a named primary key. */
        protected static final Pattern PK_NAMED_PATTERN =
            Pattern.compile(
                ".*CONSTRAINT\\s*(.*?)\\s*PRIMARY\\s+KEY\\s*\\((.*?)\\).*",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

        String table;
        String pkName = null;
        String[] pkColumns = null;

        public PrimaryKeyFinder(Connection connection, String table) throws SQLException {
            this.table = table;

            // specific handling for sqlite_schema and synonyms, so that
            // getExportedKeys/getPrimaryKeys return an empty ResultSet instead of throwing an
            // exception
            if ("sqlite_schema".equals(table) || "sqlite_master".equals(table)) return;

            if (table == null || table.trim().isEmpty()) {
                throw new SQLException("Invalid table name: '" + this.table + "'");
            }

            try (ResultSet rs = executeQuery(
                connection,
                 "select sql from sqlite_schema where"
                 + " lower(name) = lower('"
                 + escape(table)
                 + "') and type in ('table', 'view')")) {

                if (!rs.next()) {
                    throw new SQLException("Table not found: '" + table + "'");
                }

                Matcher matcher = PK_NAMED_PATTERN.matcher(rs.getString(1));
                if (matcher.find()) {
                    pkName = unquoteIdentifier(escape(matcher.group(1)));
                    pkColumns = matcher.group(2).split(",");
                } else {
                    matcher = PK_UNNAMED_PATTERN.matcher(rs.getString(1));
                    if (matcher.find()) {
                        pkColumns = matcher.group(1).split(",");
                    }
                }

                if (pkColumns == null) {
                    try (ResultSet rs2 = executeQuery(connection, "pragma table_info('" + escape(table) + "')")) {
                        while (rs2.next()) {
                            if (rs2.getBoolean(6)) pkColumns = new String[] {rs2.getString(2)};
                        }
                    }
                }

                if (pkColumns != null) {
                    for (int i = 0; i < pkColumns.length; i++) {
                        pkColumns[i] = unquoteIdentifier(pkColumns[i]);
                    }
                }
            }
        }

        /** @return The primary key name if any. */
        public String getName() {
            return pkName;
        }

        /** @return Array of primary key column(s) if any. */
        public String[] getColumns() {
            return pkColumns;
        }
    }

    /**
     * Original algorithm taken from Xerial SQLite driver.
     */
    static class ImportedKeyFinder {

        /** Pattern used to extract a named primary key. */
        private final Pattern FK_NAMED_PATTERN =
            Pattern.compile(
                "CONSTRAINT\\s*\"?([A-Za-z_][A-Za-z\\d_]*)?\"?\\s*FOREIGN\\s+KEY\\s*\\((.*?)\\)",
                Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

        private final Connection conn;
        private final List<ForeignKey> fkList = new ArrayList<>();

        public ImportedKeyFinder(Connection connection, String table) throws SQLException {
            this.conn = connection;
            if (table == null || table.trim().isEmpty()) {
                throw new SQLException("Invalid table name: '" + table + "'");
            }

            List<String> fkNames = getForeignKeyNames(table);

            try (ResultSet rs = executeQuery(connection,
                         "pragma foreign_key_list('"
                         + escape(table.toLowerCase())
                         + "')")) {

                int prevFkId = -1;
                int count = 0;
                ForeignKey fk = null;
                while (rs.next()) {
                    int fkId = rs.getInt(1);
                    String pkTableName = rs.getString(3);
                    String fkColName = rs.getString(4);
                    String pkColName = rs.getString(5);
                    String onUpdate = rs.getString(6);
                    String onDelete = rs.getString(7);
                    String match = rs.getString(8);

                    String fkName = null;
                    if (fkNames.size() > count) fkName = fkNames.get(count);

                    if (fkId != prevFkId) {
                        fk =
                            new ForeignKey(
                                fkName,
                                pkTableName,
                                table,
                                onUpdate,
                                onDelete,
                                match);
                        fkList.add(fk);
                        prevFkId = fkId;
                        count++;
                    }
                    if (fk != null) {
                        fk.addColumnMapping(fkColName, pkColName);
                    }
                }
            }
        }

        private List<String> getForeignKeyNames(String tbl) throws SQLException {
            List<String> fkNames = new ArrayList<>();
            if (tbl == null) {
                return fkNames;
            }
            try (ResultSet rs = executeQuery(conn,
                         "select sql from sqlite_schema where"
                         + " lower(name) = lower('"
                         + escape(tbl)
                         + "')")) {

                if (rs.next()) {
                    Matcher matcher = FK_NAMED_PATTERN.matcher(rs.getString(1));

                    while (matcher.find()) {
                        fkNames.add(matcher.group(1));
                    }
                }
            }
            Collections.reverse(fkNames);
            return fkNames;
        }

        public List<ForeignKey> getFkList() {
            return fkList;
        }

        static class ForeignKey {

            private final String fkName;
            private final String pkTableName;
            private final String fkTableName;
            private final List<String> fkColNames = new ArrayList<>();
            private final List<String> pkColNames = new ArrayList<>();
            private final String onUpdate;
            private final String onDelete;
            private final String match;

            ForeignKey(
                String fkName,
                String pkTableName,
                String fkTableName,
                String onUpdate,
                String onDelete,
                String match) {
                this.fkName = fkName;
                this.pkTableName = pkTableName;
                this.fkTableName = fkTableName;
                this.onUpdate = onUpdate;
                this.onDelete = onDelete;
                this.match = match;
            }

            public String getFkName() {
                return fkName;
            }

            void addColumnMapping(String fkColName, String pkColName) {
                fkColNames.add(fkColName);
                pkColNames.add(pkColName);
            }

            @Override
            public String toString() {
                return "ForeignKey [fkName=" + fkName + ", pkTableName=" + pkTableName + ", fkTableName=" + fkTableName + ", pkColNames=" + pkColNames + ", fkColNames=" + fkColNames + "]";
            }
        }
    }

}
