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
public class OracleSQLGenerator extends SQLGenerator {

    @Override
    public String getDropSQL(String prefix, String destinationTableName) {
        StringBuilder result = new StringBuilder("DROP TABLE ");
        result.append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName));
        result.append(" CASCADE CONSTRAINTS PURGE");
        return result.toString();
    }

    @Override
    public String getCreateSQL(String prefix, CubeSpecification cubeSpec, String destinationTableName) {
        StringBuilder result = new StringBuilder("CREATE TABLE ");
        result.append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName));
        result.append("( ");
        for (String s : cubeSpec.getDistinctHierarchyColumnList()) {
            result.append(s).append(" VARCHAR2(1000 BYTE), ");
        }
        for (String s : cubeSpec.getDistinctMeasureColumnList()) {
            result.append(s).append(" NUMBER, ");
        }
        return result.substring(0, result.length() - 2) + ")";
    }

    @Override
    public String getLoadSQLSubSelectColumnList(CubeSpecification cubeSpec) {
        List<String> dimensionColumnList = new ArrayList<>();
        StringBuilder result = new StringBuilder();
        for (HierarchySpecification hier : cubeSpec.getHierarchies()) {
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
            result.append(" nvl(to_char(").append(column).append("), 'N/A') ").append(column).append(", ");
        }
        for (String column : cubeSpec.getDistinctMeasureColumnList()) {
            result.append(" nvl(").append(column).append(",0) ").append(column).append(", ");
        }

        return result.substring(0, result.length() - 2);
    }

}
