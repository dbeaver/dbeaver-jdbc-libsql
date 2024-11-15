/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Queries related to keys and indexes were taken from Xerial SQLite driver (https://github.com/xerial/sqlite-jdbc)
 */
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
            HttpURLConnection con = connection.getClient().openSimpleConnection("version");
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
        if (CommonUtils.isEmpty(tableName) || "%".equals(tableName)) {
            tableName = null;
        }
        return executeQuery(
            "WITH all_tables AS (SELECT name AS tn FROM sqlite_master WHERE type = 'table'" +
                (tableName == null ? "" : " and name=" + LibSqlUtils.quote(tableName)) + ") \n" +
                "SELECT " +
                    "NULL as TABLE_CAT, " +
                    "NULL AS TABLE_SCHEM, " +
                    "at.tn as TABLE_NAME,\n" +
                    "pti.name as COLUMN_NAME," +
                    Types.VARCHAR + " AS DATA_TYPE," +
                    "pti.type AS TYPE_NAME," +
                    "0 AS COLUMN_SIZE," +
                    "0 as COLUMN_SIZE," +
                    "0 as BUFFER_LENGTH," +
                    "0 as DECIMAL_DIGITS,\n" +
                    columnNullable + " as NULLABLE," +
                    "NULL as REMARKS," +
                    "NULL as COLUMN_DEF," +
                    "0 as SQL_DATA_TYPE," +
                    "0 as SQL_DATETIME_SUB," +
                    "0 as CHAR_OCTET_LENGTH," +
                    "pti.cid as ORDINAL_POSITION," +
                    "'' as IS_NULLABLE," +
                    "NULL as SCOPE_CATALOG," +
                    "NULL as SCOPE_SCHEMA," +
                    "NULL as SCOPE_TABLE," +
                    "NULL as SOURCE_DATA_TYPE," +
                    "'' as IS_AUTOINCREMENT," +
                    "'' as IS_GENERATEDCOLUMN\n" +
                "FROM all_tables at INNER JOIN pragma_table_info(at.tn) pti\n" +
                "ORDER BY TABLE_NAME");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String tableName) throws SQLException {
        String table = tableName;
        PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(connection, table);
        String[] columns = pkFinder.getColumns();

        StringBuilder sql = new StringBuilder();
        sql.append("select null as TABLE_CAT, null as TABLE_SCHEM, '")
            .append(LibSqlUtils.escape(table))
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
                .append(pkName).append(" as pk, '")
                .append(LibSqlUtils.escape(LibSqlUtils.unquote(columns[i]))).append("' as cn, ")
                .append(i + 1).append(" as ks");
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
                .append(LibSqlUtils.escape(table))
                .append("' as TABLE_NAME, un as NON_UNIQUE, null as INDEX_QUALIFIER, n as INDEX_NAME, ")
                .append(Integer.toString(DatabaseMetaData.tableIndexOther)).append(" as TYPE, op as ORDINAL_POSITION, ")
                .append("cn as COLUMN_NAME, null as ASC_OR_DESC, 0 as CARDINALITY, 0 as PAGES, null as FILTER_CONDITION from (");

            // this always returns a result set now, previously threw exception
            List<IndexInfo> indexList = new ArrayList<>();
            try (ResultSet rs = executeQuery("pragma index_list('" + LibSqlUtils.escape(table) + "')")) {
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
                    try (ResultSet rs = executeQuery("pragma index_info('" + LibSqlUtils.escape(indexName) + "')")) {
                        while (rs.next()) {
                            StringBuilder sqlRow = new StringBuilder();

                            String colName = rs.getString(3);
                            sqlRow.append("select ")
                                .append(1 - currentIndex.indexId).append(" as un,'")
                                .append(LibSqlUtils.escape(indexName)).append("' as n,")
                                .append(rs.getInt(1) + 1).append(" as op,");
                            if (colName == null) { // expression index
                                sqlRow.append("null");
                            } else {
                                sqlRow.append("'").append(LibSqlUtils.escape(colName)).append("'");
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
                .append(LibSqlUtils.quote(catalog)).append(" as PKTABLE_CAT, ")
                .append(LibSqlUtils.quote(schema)).append(" as PKTABLE_SCHEM, ")
                .append("ptn as PKTABLE_NAME, pcn as PKCOLUMN_NAME, ")
                .append(LibSqlUtils.quote(catalog)).append(" as FKTABLE_CAT, ")
                .append(LibSqlUtils.quote(schema)).append(" as FKTABLE_SCHEM, ")
                .append(LibSqlUtils.quote(table)).append(" as FKTABLE_NAME, ")
                .append("fcn as FKCOLUMN_NAME, ks as KEY_SEQ, ur as UPDATE_RULE, dr as DELETE_RULE, fkn as FK_NAME, pkn as PK_NAME, ")
                .append(DatabaseMetaData.importedKeyInitiallyDeferred)
                .append(" as DEFERRABILITY from (");

            // Use a try catch block to avoid "query does not return ResultSet" error
            try (ResultSet rs = executeQuery("pragma foreign_key_list('" + LibSqlUtils.escape(table) + "')")) {

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
                        .append(LibSqlUtils.escape(PKTabName)).append("' as ptn, '")
                        .append(LibSqlUtils.escape(FKColName)).append("' as fcn, '")
                        .append(LibSqlUtils.escape(PKColName)).append("' as pcn,")
                        .append("case '")
                        .append(LibSqlUtils.escape(updateRule))
                        .append("'")
                        .append(" when 'NO ACTION' then ").append(DatabaseMetaData.importedKeyNoAction)
                        .append(" when 'CASCADE' then ").append(DatabaseMetaData.importedKeyCascade)
                        .append(" when 'RESTRICT' then ").append(DatabaseMetaData.importedKeyRestrict)
                        .append(" when 'SET NULL' then ").append(DatabaseMetaData.importedKeySetNull)
                        .append(" when 'SET DEFAULT' then ").append(DatabaseMetaData.importedKeySetDefault)
                        .append(" end as ur, ")
                        .append("case '")
                        .append(LibSqlUtils.escape(deleteRule))
                        .append("'")
                        .append(" when 'NO ACTION' then ").append(DatabaseMetaData.importedKeyNoAction)
                        .append(" when 'CASCADE' then ").append(DatabaseMetaData.importedKeyCascade)
                        .append(" when 'RESTRICT' then ").append(DatabaseMetaData.importedKeyRestrict)
                        .append(" when 'SET NULL' then ").append(DatabaseMetaData.importedKeySetNull)
                        .append(" when 'SET DEFAULT' then ").append(DatabaseMetaData.importedKeySetDefault)
                        .append(" end as dr, ")
                        .append(fkName == null ? "''" : LibSqlUtils.quote(fkName)).append(" as fkn, ")
                        .append(pkName == null ? "''" : LibSqlUtils.quote(pkName)).append(" as pkn");
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

    private static final Map<String, Integer> RULE_MAP = new HashMap<>();

    static {
        RULE_MAP.put("NO ACTION", DatabaseMetaData.importedKeyNoAction);
        RULE_MAP.put("CASCADE", DatabaseMetaData.importedKeyCascade);
        RULE_MAP.put("RESTRICT", DatabaseMetaData.importedKeyRestrict);
        RULE_MAP.put("SET NULL", DatabaseMetaData.importedKeySetNull);
        RULE_MAP.put("SET DEFAULT", DatabaseMetaData.importedKeySetDefault);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(connection, table);
        String[] pkColumns = pkFinder.getColumns();

        catalog = (catalog != null) ? LibSqlUtils.quote(catalog) : null;
        schema = (schema != null) ? LibSqlUtils.quote(schema) : null;

        StringBuilder exportedKeysQuery = new StringBuilder();

        String target = null;
        int count = 0;
        if (pkColumns != null) {
            // retrieve table list
            ArrayList<String> tableList;
            try (ResultSet rs = executeQuery("select name from sqlite_schema where type = 'table'")) {
                tableList = new ArrayList<>();

                while (rs.next()) {
                    String tblname = rs.getString(1);
                    tableList.add(tblname);
                    if (tblname.equalsIgnoreCase(table)) {
                        // get the correct case as in the database
                        // (not uppercase nor lowercase)
                        target = tblname;
                    }
                }
            }

            // find imported keys for each table
            for (String tbl : tableList) {
                final ImportedKeyFinder impFkFinder = new ImportedKeyFinder(connection, tbl);
                List<ImportedKeyFinder.ForeignKey> fkNames = impFkFinder.getFkList();

                for (ImportedKeyFinder.ForeignKey foreignKey : fkNames) {
                    String PKTabName = foreignKey.pkTableName;

                    if (PKTabName == null || !PKTabName.equalsIgnoreCase(target)) {
                        continue;
                    }

                    for (int j = 0; j < foreignKey.fkColNames.size(); j++) {
                        int keySeq = j + 1;
                        String pkColName = foreignKey.pkColNames.get(j);
                        pkColName = (pkColName == null) ? "" : pkColName;
                        String fkColName = foreignKey.fkColNames.get(j);
                        fkColName = (fkColName == null) ? "" : fkColName;

                        boolean usePkName = false;
                        for (String pkColumn : pkColumns) {
                            if (pkColumn != null && pkColumn.equalsIgnoreCase(pkColName)) {
                                usePkName = true;
                                break;
                            }
                        }
                        String pkName =
                            (usePkName && pkFinder.getName() != null) ? pkFinder.getName() : "";

                        exportedKeysQuery
                            .append(count > 0 ? " union all select " : "select ")
                            .append(keySeq).append(" as ks, '")
                            .append(LibSqlUtils.escape(tbl)).append("' as fkt, '")
                            .append(LibSqlUtils.escape(fkColName)).append("' as fcn, '")
                            .append(LibSqlUtils.escape(pkColName)).append("' as pcn, '")
                            .append(LibSqlUtils.escape(pkName)).append("' as pkn, ")
                            .append(RULE_MAP.get(foreignKey.onUpdate)).append(" as ur, ")
                            .append(RULE_MAP.get(foreignKey.onDelete)).append(" as dr, ");

                        String fkName = foreignKey.getFkName();

                        if (fkName != null) {
                            exportedKeysQuery.append("'").append(LibSqlUtils.escape(fkName)).append("' as fkn");
                        } else {
                            exportedKeysQuery.append("'' as fkn");
                        }

                        count++;
                    }
                }
            }
        }

        boolean hasImportedKey = (count > 0);
        StringBuilder sql = new StringBuilder(512);
        sql.append("select ")
            .append(catalog).append(" as PKTABLE_CAT, ")
            .append(schema).append(" as PKTABLE_SCHEM, ")
            .append(LibSqlUtils.quote(target)).append(" as PKTABLE_NAME, ")
            .append(hasImportedKey ? "pcn" : "''").append(" as PKCOLUMN_NAME, ")
            .append(catalog).append(" as FKTABLE_CAT, ")
            .append(schema).append(" as FKTABLE_SCHEM, ")
            .append(hasImportedKey ? "fkt" : "''").append(" as FKTABLE_NAME, ")
            .append(hasImportedKey ? "fcn" : "''").append(" as FKCOLUMN_NAME, ")
            .append(hasImportedKey ? "ks" : "-1").append(" as KEY_SEQ, ")
            .append(hasImportedKey ? "ur" : "3").append(" as UPDATE_RULE, ")
            .append(hasImportedKey ? "dr" : "3").append(" as DELETE_RULE, ")
            .append(hasImportedKey ? "fkn" : "''").append(" as FK_NAME, ")
            .append(hasImportedKey ? "pkn" : "''").append(" as PK_NAME, ")
            .append(DatabaseMetaData.importedKeyInitiallyDeferred).append(" as DEFERRABILITY ");

        if (hasImportedKey) {
            sql.append("from (")
                .append(exportedKeysQuery)
                .append(") ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ");
        } else {
            sql.append("limit 0");
        }

        return executeQuery(sql.toString());
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
            "select " + LibSqlUtils.quote(parentCatalog)
            + " as PKTABLE_CAT, " + LibSqlUtils.quote(parentSchema)
            + " as PKTABLE_SCHEM, " + LibSqlUtils.quote(parentTable)
            + " as PKTABLE_NAME, "
            + "'' as PKCOLUMN_NAME, " + LibSqlUtils.quote(foreignCatalog)
            + " as FKTABLE_CAT, " + LibSqlUtils.quote(foreignSchema)
            + " as FKTABLE_SCHEM, " + LibSqlUtils.quote(foreignTable)
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

    private ResultSet executeQuery(String query) throws SQLException {
        return LibSqlUtils.executeQuery(connection, query);
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

            try (ResultSet rs = LibSqlUtils.executeQuery(
                connection,
                "select sql from sqlite_schema where"
                + " lower(name) = lower('"
                + LibSqlUtils.escape(table)
                + "') and type in ('table', 'view')")) {

                if (!rs.next()) {
                    throw new SQLException("Table not found: '" + table + "'");
                }

                Matcher matcher = PK_NAMED_PATTERN.matcher(rs.getString(1));
                if (matcher.find()) {
                    pkName = LibSqlUtils.unquote(LibSqlUtils.escape(matcher.group(1)));
                    pkColumns = matcher.group(2).split(",");
                } else {
                    matcher = PK_UNNAMED_PATTERN.matcher(rs.getString(1));
                    if (matcher.find()) {
                        pkColumns = matcher.group(1).split(",");
                    }
                }

                if (pkColumns == null) {
                    try (ResultSet rs2 = LibSqlUtils.executeQuery(connection, "pragma table_info('" + LibSqlUtils.escape(table) + "')")) {
                        while (rs2.next()) {
                            if (rs2.getBoolean(6)) pkColumns = new String[] {rs2.getString(2)};
                        }
                    }
                }

                if (pkColumns != null) {
                    for (int i = 0; i < pkColumns.length; i++) {
                        pkColumns[i] = LibSqlUtils.unquote(pkColumns[i]);
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
        private static final Pattern FK_NAMED_PATTERN =
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

            try (ResultSet rs = LibSqlUtils.executeQuery(connection,
                "pragma foreign_key_list('"
                + LibSqlUtils.escape(table.toLowerCase())
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
            try (ResultSet rs = LibSqlUtils.executeQuery(conn,
                "select sql from sqlite_schema where"
                + " lower(name) = lower('"
                + LibSqlUtils.escape(tbl)
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
