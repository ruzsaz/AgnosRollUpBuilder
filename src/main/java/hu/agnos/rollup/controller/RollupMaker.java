/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hu.agnos.rollup.controller;

import hu.agnos.cube.specification.entity.CubeSpecification;
import hu.agnos.cube.specification.entity.HierarchySpecification;
import hu.agnos.cube.specification.entity.LevelSpecification;
import hu.agnos.rollup.controller.service.H2SQLGenerator;
import hu.agnos.rollup.controller.service.OracleSQLGenerator;
import hu.agnos.rollup.controller.service.PostgreSQLGenerator;
import hu.agnos.rollup.controller.service.SAPSQLGenerator;
import hu.agnos.rollup.controller.service.SQLGenerator;
import hu.agnos.rollup.controller.service.SQLServerSQLGenerator;
import hu.agnos.rollup.db.util.DBService;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author parisek
 */
public class RollupMaker {

    private final Logger logger;

    private final CubeSpecification CUBE_SPEC;

    private final int OLAP_THRESHOLD_ALL_TABLE_ROW_COUNT = 100;
    private final int OLAP_THRESHOLD_HIER_ROW_COUNT = 10;

    private final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private final String H2_DRIVER = "org.h2.Driver";
    private final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private final String POSTGRES_DRIVER = "org.postgresql.Driver";
    private final String SAP_DRIVER = "com.sap.db.jdbc.Driver";

    private SQLGenerator sqlGenerator;
    private List<String> partitionedHierchiesList;

    public RollupMaker(CubeSpecification cube) {
        logger = LoggerFactory.getLogger(RollupMaker.class);
        this.CUBE_SPEC = cube;
        this.partitionedHierchiesList = new ArrayList<>();
        switch (cube.getSourceDBDriver()) {
            case ORACLE_DRIVER:
                this.sqlGenerator = new OracleSQLGenerator();
                break;
            case H2_DRIVER:
                this.sqlGenerator = new H2SQLGenerator();
                break;
            case SQLSERVER_DRIVER:
                this.sqlGenerator = new SQLServerSQLGenerator();
                break;

            case POSTGRES_DRIVER:
                this.sqlGenerator = new PostgreSQLGenerator();
                break;
            case SAP_DRIVER:
                this.sqlGenerator = new SAPSQLGenerator();
                break;

        }
    }

    private void makeAllHierarchiesRollupInDestinationTable(String destinationTableName, String sourceTableName) {
        String dbUser = CUBE_SPEC.getSourceDBUser();
        String dbPassword = CUBE_SPEC.getSourceDBPassword();
        String dbUrl = CUBE_SPEC.getSourceDBURL();
        String dbDriver = CUBE_SPEC.getSourceDBDriver();

        for (String dimName : partitionedHierchiesList) {
            System.out.println("######################################");
            System.out.println(dimName);
            System.out.println("######################################");

            String sql = this.sqlGenerator.getDropSQL("TMP_", destinationTableName);
            logger.info(sql);
            DBService.slientExecuteQuery(sql, CUBE_SPEC.getSourceDBUser(), CUBE_SPEC.getSourceDBPassword(), CUBE_SPEC.getSourceDBURL(), CUBE_SPEC.getSourceDBDriver());

            sql = this.sqlGenerator.getCreateSQL("TMP_", CUBE_SPEC, destinationTableName);
            logger.info(sql);
            DBService.executeQuery(sql, dbUser, dbPassword, dbUrl, dbDriver);

            List<LevelSpecification> levels = new ArrayList<>();

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            sql = this.sqlGenerator.getDropSQL("TMP_", destinationTableName);
            logger.info(sql);
            DBService.slientExecuteQuery(sql, dbUser, dbPassword, dbUrl, dbDriver);

            sql = this.sqlGenerator.getCreateSQL("TMP_", CUBE_SPEC, destinationTableName);
            logger.info(sql);
            DBService.executeQuery(sql, dbUser, dbPassword, dbUrl, dbDriver);

            makeOneHierRollupInTmpTable(dimName,
                    CUBE_SPEC.getHierarchyByName(dimName), destinationTableName);

            sql = this.sqlGenerator.getSelectQueryToLoadOneHierarchyRollup("TMP_", CUBE_SPEC, destinationTableName, sourceTableName);
            DBService.executeQuery(sql, dbUser, dbPassword, dbUrl, dbDriver);
            logger.info(sql);

            sql = this.sqlGenerator.getDropSQL("TMP_", destinationTableName);
            DBService.slientExecuteQuery(sql, dbUser, dbPassword, dbUrl, dbDriver);
            logger.info(sql);
        }

    }

    private void makeOneHierRollupInTmpTable(String dimName, HierarchySpecification hier, String destinationTableName) {
        List<String> hierColumnList = hier.getColumnListToOLAPBuilding();
        List<String> allDimensionColumnListMinusHierarchyColumnList = CUBE_SPEC.getDistinctHierarchyColumnList();
        allDimensionColumnListMinusHierarchyColumnList.removeAll(hierColumnList);
        List<String> measureColumnList = CUBE_SPEC.getDistinctMeasureColumnList();

        if (hier != null) {
            for (int i = 0; i < hier.getOlapSqlIterationCnt(); i++) {
                StringBuilder olapSQL = new StringBuilder("SELECT ");
                String selectStatement = hier.getOlapSelectStatementByIteration(i);
                olapSQL.append(selectStatement).append(", ");
                for (String otherDimensionColumnName : allDimensionColumnListMinusHierarchyColumnList) {
                    olapSQL.append(otherDimensionColumnName).append(", ");
                }

                String toInsertStatement = olapSQL.substring(0, olapSQL.length() - 2);

                for (String measureColumnName : measureColumnList) {
                    String aggFunction = CUBE_SPEC.getAggregationFunctionByName(hier.getUniqueName(), measureColumnName);
                    olapSQL.append(aggFunction).append("(").append(measureColumnName).append("), ");
                }
                olapSQL = new StringBuilder(olapSQL.substring(0, olapSQL.length() - 2));
                olapSQL.append("\nFROM ").append(destinationTableName).append("\n");
                olapSQL.append("GROUP BY ");

                String groupByStatement = hier.getOlapGroupByStatementByIteration(i);
                olapSQL.append(groupByStatement);

                if (groupByStatement != null && !groupByStatement.isEmpty()) {
                    olapSQL.append(", ");
                }

                for (String otherDimensionColumnName : allDimensionColumnListMinusHierarchyColumnList) {
                    olapSQL.append(otherDimensionColumnName).append(", ");
                }

                olapSQL = new StringBuilder(olapSQL.substring(0, olapSQL.length() - 2));

                String sql = getInsertStatementFromSeletcStatement(toInsertStatement, destinationTableName) + olapSQL.toString();
                logger.info(sql);
                DBService.executeQuery(sql, CUBE_SPEC.getSourceDBUser(), CUBE_SPEC.getSourceDBPassword(), CUBE_SPEC.getSourceDBURL(), CUBE_SPEC.getSourceDBDriver());
            }
        }
    }

    private String getInsertStatementFromSeletcStatement(String selectStatement, String destinationTableName) {
        StringBuilder result = new StringBuilder("INSERT INTO ");
        result.append(this.sqlGenerator.getFullyQualifiedTableNameWithPrefix("TMP_", destinationTableName)).append(" (");
        String[] columnList = selectStatement.split(",");
        for (int i = 0; i < columnList.length; i++) {
            String column = columnList[i];
            if (i == 0) {
                column = column.substring(6);
            }
            column = column.replaceAll("'All' as", "");
            column = column.trim();
            result.append(column).append(", ");
        }

        for (String measureColumnName : CUBE_SPEC.getDistinctMeasureColumnList()) {
            result.append(measureColumnName).append(", ");
        }
        return result.substring(0, result.length() - 2) + ")\n";

    }

    public void make(String destinationTableName, String sourceTableName) {
        this.partitionedHierchiesList = CUBE_SPEC.getPartitionedHierarchyList();
        logger.info("Start");

        init(destinationTableName, sourceTableName);
        loadBaseLevel(destinationTableName, sourceTableName);
        makeAllHierarchiesRollupInDestinationTable(destinationTableName, sourceTableName);

    }

    private void init(String destinationTableName, String sourceTableName) {
        String sql = this.sqlGenerator.getDropSQL(null, destinationTableName);
        logger.info(sql);
        DBService.slientExecuteQuery(sql, CUBE_SPEC.getSourceDBUser(), CUBE_SPEC.getSourceDBPassword(), CUBE_SPEC.getSourceDBURL(), CUBE_SPEC.getSourceDBDriver());

        sql = this.sqlGenerator.getCreateSQL(null, CUBE_SPEC, destinationTableName);
        logger.info(sql);
        DBService.executeQuery(sql, CUBE_SPEC.getSourceDBUser(), CUBE_SPEC.getSourceDBPassword(), CUBE_SPEC.getSourceDBURL(), CUBE_SPEC.getSourceDBDriver());

    }

    private void loadBaseLevel(String destinationTableName, String sourceTableName) {
        String sql = this.sqlGenerator.getSQLToBaseLevelDataLoading(null, CUBE_SPEC, destinationTableName, sourceTableName);
        logger.info(sql);
        DBService.executeQuery(sql, CUBE_SPEC.getSourceDBUser(), CUBE_SPEC.getSourceDBPassword(), CUBE_SPEC.getSourceDBURL(), CUBE_SPEC.getSourceDBDriver());
    }
}
