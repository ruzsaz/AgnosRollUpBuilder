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
public class OracleSQLGenerator extends SQLGenerator {

    @Override
    public String getDropSQL(String prefix, String destinationTableName) {
        StringBuilder result = new StringBuilder("DROP TABLE ");
        result.append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName));
        result.append(" CASCADE CONSTRAINTS PURGE");
        return result.toString();
    }

    @Override
    public String getCreateSQL(String prefix, CubeSpecification cube, String destinationTableName) {
        StringBuilder result = new StringBuilder("CREATE TABLE ");
        result.append(getFullyQualifiedTableNameWithPrefix(prefix, destinationTableName));
        result.append("( ");
        for (String s : cube.getDistinctDimensionColumnList()) {
            result.append(s).append(" VARCHAR2(1000 BYTE), ");
        }
        Optional<MeasureSpecification> m = cube.getCountDistinctMeasure();
        if (!m.isEmpty()) {
            result
                    .append(m.get().getUniqueName())
                    .append(" NUMBER, ");
        } else {

            for (String s : cube.getDistinctClassicalMeasureNameList()) {
                result.append(s).append(" NUMBER, ");
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
            stringBuilder.append(" nvl(to_char(").append(column).append("), 'N/A') ").append(column).append(", ");
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
                        .append(" nvl(")
                        .append(column)
                        .append(",0) ")
                        .append(column)
                        .append(", ");
            }
        }
        return  stringBuilder.substring(0, stringBuilder.length() - 2);
    }

    @Override
    public String getRenameSQL(String prefix, String destinationTableName) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public String getCountDistinctAggregateFunctionForVirtualMeasureSQL(String virtualColumName) {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

}
