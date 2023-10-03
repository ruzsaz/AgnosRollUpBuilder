/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hu.agnos.rollup.service.sql;

import hu.agnos.cube.specification.entity.CubeSpecification;
import hu.agnos.cube.specification.entity.HierarchySpecification;
import hu.agnos.cube.specification.entity.LevelSpecification;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author parisek
 */
public abstract class SQLGenerator {

    public abstract String getDropSQL(String prefix, String destinationTableName);

    public abstract String getCreateSQL(String prefix, CubeSpecification cube, String destinationTableName);

    public abstract String getLoadSQLSubSelectColumnList(CubeSpecification cube);

    public String getLoadSQLSelectColumnList(CubeSpecification cube) {
        List<String> dimensionColumnList = new ArrayList<>();
        StringBuilder result = new StringBuilder();
        for (HierarchySpecification hier : cube.getHierarchies()) {
            for (LevelSpecification level : hier.getLevels()) {

                String columnName = level.getCodeColumnSourceName();

                if (!dimensionColumnList.contains(columnName)) {
                    dimensionColumnList.add(columnName);
                }
                if (!level.getNameColumnName().equals(level.getCodeColumnSourceName()) && !dimensionColumnList.contains(level.getNameColumnName())) {
                    dimensionColumnList.add(level.getNameColumnName());
                }
            }
        }

        for (String column : dimensionColumnList) {
            result.append(column).append(", ");
        }

        for (String column : cube.getDistinctMeasureColumnList()) {
            result.append("SUM(").append(column).append("), ");
        }

        return result.substring(0, result.length() - 2);
    }

    public String getLoadSQLGroupBYColumnList(CubeSpecification cube) {
        List<String> dimensionColumnList = new ArrayList<>();
        StringBuilder result = new StringBuilder();
        for (HierarchySpecification hier : cube.getHierarchies()) {
            for (LevelSpecification level : hier.getLevels()) {

                String columnName = level.getCodeColumnSourceName();
                if (!dimensionColumnList.contains(columnName)) {
                    dimensionColumnList.add(columnName);
                }
                if (!level.getNameColumnName().equals(level.getCodeColumnSourceName()) && !dimensionColumnList.contains(level.getNameColumnName())) {
                    dimensionColumnList.add(level.getNameColumnName());
                }
            }
        }

        for (String column : dimensionColumnList) {
            result.append(column).append(", ");
        }
        return result.substring(0, result.length() - 2);
    }

    public String getSelectQueryToLoadOneHierarchyRollup(String prefix, CubeSpecification cube, String destinationTableName, String sourceTableName) {
        StringBuilder insertQuerySQLBuilder = new StringBuilder("INSERT INTO ");
        insertQuerySQLBuilder.append(destinationTableName).append(" (");

        StringBuilder selectQuerySQLBuilder = new StringBuilder(" SELECT ");

        for (String column : cube.getDistinctHierarchyColumnList()) {
            selectQuerySQLBuilder.append(column).append(", ");
            insertQuerySQLBuilder.append(column).append(", ");

        }

        List<String> measureColumnList = cube.getDistinctMeasureColumnList();
        for (String column : measureColumnList) {
            selectQuerySQLBuilder.append(column).append(", ");
            insertQuerySQLBuilder.append(column).append(", ");
        }
        selectQuerySQLBuilder = new StringBuilder(selectQuerySQLBuilder.substring(0, selectQuerySQLBuilder.length() - 2));
        insertQuerySQLBuilder = new StringBuilder(insertQuerySQLBuilder.substring(0, insertQuerySQLBuilder.length() - 2));
        insertQuerySQLBuilder.append(")");
        selectQuerySQLBuilder.append(" FROM ");
        selectQuerySQLBuilder.append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName));
        insertQuerySQLBuilder.append(selectQuerySQLBuilder.toString());
        return insertQuerySQLBuilder.toString();
    }

    public String getFullyQualifiedTableNameWithPrefix(String prefix, String destinationTableName) {
        StringBuilder result = new StringBuilder();
        if (prefix != null) {
            if (destinationTableName.contains(".")) {
                String[] tableNameSegments = destinationTableName.split("\\.");
                for (int i = 0; i < tableNameSegments.length; i++) {
                    if (i == tableNameSegments.length - 1) {
                        result.append(getQuotedTableNameWithPrefix(prefix, tableNameSegments[i]));
                    } else {
                        result.append(tableNameSegments[i]);
                        result.append(".");
                    }
                }
            } else {
                result.append(getQuotedTableNameWithPrefix(prefix, destinationTableName));
            }
        } else {
            result.append(destinationTableName);
        }
        return result.toString();
    }

    private String getQuotedTableNameWithPrefix(String prefix, String tableName) {
        StringBuilder result = new StringBuilder();
        if (prefix != null) {
            if (tableName.startsWith("[")) {
                result.append("[");
                result.append(prefix);
                result.append(tableName.substring(1));
            } else if (tableName.startsWith("\"")) {
                result.append("\"");
                result.append(prefix);
                result.append(tableName.substring(1));
            } else {
                result.append(prefix);
                result.append(tableName);
            }
        } else {
            result.append(tableName);
        }
        return result.toString();
    }

    public String getLoadSQLInsertColumnList(CubeSpecification cube) {
        List<String> dimensionColumnList = new ArrayList<>();
        StringBuilder result = new StringBuilder();
        for (HierarchySpecification hier : cube.getHierarchies()) {
            for (LevelSpecification level : hier.getLevels()) {
                if (!dimensionColumnList.contains(level.getCodeColumnSourceName())) {
                    dimensionColumnList.add(level.getCodeColumnSourceName());
                }
                if (!level.getNameColumnName().equals(level.getCodeColumnSourceName()) && !dimensionColumnList.contains(level.getNameColumnName())) {
                    dimensionColumnList.add(level.getNameColumnName());
                }
            }
        }

        for (String column : dimensionColumnList) {
            result.append(column).append(", ");
        }

        for (String column : cube.getDistinctMeasureColumnList()) {
            result.append(column).append(", ");
        }

        return result.substring(0, result.length() - 2);
    }

    public String getSQLToBaseLevelDataLoading(String prefix, CubeSpecification cube, String destinationTableName, String sourceTableName) {
        StringBuilder result = new StringBuilder("INSERT INTO ");
        result.append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName));
        result.append(" ( ");
        result.append(getLoadSQLInsertColumnList(cube));
        result.append(" ) SELECT ").append(getLoadSQLSelectColumnList(cube));
        result.append(" FROM ");
        result.append("( ").append(getLoadSQLSubSelect(prefix, cube, sourceTableName)).append(") as foo");
        result.append(" GROUP BY ").append(getLoadSQLGroupBYColumnList(cube));
        return result.toString();
    }

    public String getLoadSQLSubSelect(String prefix, CubeSpecification cube, String sourceTableName) {
        StringBuilder result = new StringBuilder("SELECT ");
        result.append(getLoadSQLSubSelectColumnList(cube));

        result.append(" FROM ");
        result.append(getFullyQualifiedTableNameWithPrefix(prefix, sourceTableName));
        return result.toString();
    }

}
