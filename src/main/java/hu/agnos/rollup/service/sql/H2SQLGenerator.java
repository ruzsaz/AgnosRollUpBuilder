/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hu.agnos.rollup.service.sql;

import java.util.ArrayList;
import java.util.List;

import hu.agnos.cube.specification.entity.CubeSpecification;
import hu.agnos.cube.specification.entity.DimensionSpecification;
import hu.agnos.cube.specification.entity.LevelSpecification;

/**
 *
 * @author parisek
 */
public class H2SQLGenerator extends SQLGenerator {

    @Override
    public String getDropSQL(String prefix, String destinationTableName) {
        StringBuilder result = new StringBuilder("DROP TABLE ");
        result.append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName));
        return result.toString();
    }

    @Override
    public String getCreateSQL(String prefix, CubeSpecification cubeSpec, String destinationTableName) {
        StringBuilder result = new StringBuilder("CREATE TABLE ");
        result.append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName));
        result.append("( ");
        for (String s : cubeSpec.getDistinctDimensionColumnList()) {
            result.append(s).append(" VARCHAR(500), ");
        }
        for (String s : cubeSpec.getDistinctMeasureColumnList()) {
            result.append(s).append(" DOUBLE, ");
        }
        return result.substring(0, result.length() - 2) + ")";
    }

    @Override
    public String getLoadSQLSubSelectColumnList(CubeSpecification cubeSpec) {
        List<String> dimensionColumnList = new ArrayList<>();
        StringBuilder result = new StringBuilder();
        for (DimensionSpecification dim : cubeSpec.getDimensions()) {
            for (LevelSpecification level : dim.getLevels()) {

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
