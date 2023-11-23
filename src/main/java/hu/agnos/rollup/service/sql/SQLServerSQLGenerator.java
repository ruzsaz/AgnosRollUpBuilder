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
import hu.agnos.cube.specification.entity.MeasureSpecification;
import java.util.Optional;

/**
 *
 * @author parisek
 */
public class SQLServerSQLGenerator extends SQLGenerator {

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
        result.append("( ");
        for (String s : cube.getDistinctDimensionColumnList()) {
            result.append(s).append(" VARCHAR(500), ");
        }

        Optional<MeasureSpecification> m = cube.getCountDistinctMeasure();
        if (!m.isEmpty()) {
            result
                    .append(m.get().getUniqueName())
                    .append(" INT, ");
        } else {

            for (String s : cube.getDistinctClassicalMeasureNameList()) {
                result.append(s).append(" FLOAT, ");
            }
        }
        return result.substring(0, result.length() - 2) + ")";
    }

    @Override
    public String getLoadSQLSubSelectColumnList(CubeSpecification cube) {

        List<String> dimensionColumnList = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        for (DimensionSpecification dim : cube.getDimensions()) {
            for (LevelSpecification level : dim.getLevels()) {

                String columnName = level.getCodeColumnName();

                if (!dimensionColumnList.contains(columnName)) {
                    dimensionColumnList.add(columnName);
                }
                if (!level.getNameColumnName().equals(level.getCodeColumnName()) && !dimensionColumnList.contains(level.getNameColumnName())) {
                    dimensionColumnList.add(1, level.getNameColumnName());

                }
            }
        }
        for (String column : dimensionColumnList) {
            stringBuilder.append(" coalesce(trim(convert(char,").append(column).append(")), 'N/A') ").append(column).append(", ");
        }

        Optional<MeasureSpecification> m = cube.getCountDistinctMeasure();
        if (!m.isEmpty()) {
            stringBuilder
                    .append(" sub_bar.DenseRank AS ")
                    .append(m.get().getUniqueName())
                    .append(", ");
        } else {
            for (String column : cube.getDistinctClassicalMeasureNameList()) {
                stringBuilder
                        .append(" coalesce(")
                        .append(column)
                        .append(",0) AS ")
                        .append(column)
                        .append(", ");
            }
        }
        return stringBuilder.substring(0, stringBuilder.length() - 2);
    }

    @Override
    public String getRenameSQL(String prefix, String destinationTableName) {
        StringBuilder result = new StringBuilder("exec sp_rename '");
        result
                .append(destinationTableName)
                .append("', '")
                .append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName))
                .append("'");
        return result.toString();

    }

    @Override
    public String getCountDistinctAggregateFunctionForVirtualMeasureSQL(String virtualColumName) {
        StringBuilder result = new StringBuilder("STRING_AGG(CONVERT(NVARCHAR(max), ");
        result.append(virtualColumName)
                .append("), ',') ");
        return result.toString();
    }
}
