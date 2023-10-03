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
public class PostgreSQLGenerator extends SQLGenerator {

    @Override
    public String getDropSQL(String prefix, String destinationTableName) {
        StringBuilder result = new StringBuilder("DROP TABLE ");
        result.append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName));
        return result.toString();
    }

    @Override
    public String getCreateSQL(String prefix, CubeSpecification cube, String destinationTableName) {
        StringBuilder result = new StringBuilder("CREATE TABLE ");
        result.append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName));
        result.append(" ( ");
        for (String s : cube.getDistinctHierarchyColumnList()) {
            result.append(s).append(" VARCHAR(500), ");
        }
        for (String s : cube.getDistinctMeasureColumnList()) {
            result.append(s).append(" REAL, ");
        }
        return result.substring(0, result.length() - 2) + ")";
    }

    @Override
    public String getLoadSQLSubSelectColumnList(CubeSpecification cube) {
        List<String> dimensionColumnList = new ArrayList<>();
        StringBuilder result = new StringBuilder();
        for (HierarchySpecification hier : cube.getHierarchies()) {
            for (LevelSpecification level : hier.getLevels()) {

                String columnName = level.getCodeColumnSourceName();

                if (!dimensionColumnList.contains(columnName)) {
                    dimensionColumnList.add(columnName);
                }
                if (!level.getNameColumnName().equals(level.getCodeColumnSourceName()) && !dimensionColumnList.contains(level.getNameColumnName())) {
                    dimensionColumnList.add(1, level.getNameColumnName());

                }
            }
        }

        for (String column : dimensionColumnList) {
            result.append(" coalesce(").append(column).append(", 'N/A') ").append(column).append(", ");
        }

        for (String column : cube.getDistinctMeasureColumnList()) {
            result.append(" coalesce(").append(column).append(",0) ").append(column).append(", ");
        }

        return result.substring(0, result.length() - 2);
    }

}
