/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hu.agnos.rollup.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import hu.agnos.cube.specification.entity.CubeSpecification;
import hu.agnos.cube.specification.entity.DimensionSpecification;
import hu.agnos.cube.specification.entity.LevelSpecification;
import hu.agnos.rollup.service.sql.H2SQLGenerator;
import hu.agnos.rollup.service.sql.OracleSQLGenerator;
import hu.agnos.rollup.service.sql.PostgreSQLGenerator;
import hu.agnos.rollup.service.sql.SAPSQLGenerator;
import hu.agnos.rollup.service.sql.SQLGenerator;
import hu.agnos.rollup.service.sql.SQLServerSQLGenerator;
import hu.agnos.rollup.util.DBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author parisek
 */
public class RollupMaker {

    private final Logger logger;

    private final CubeSpecification CUBE_SPEC;

    private final int PARTITION_THRESHOLD_ALL_TABLE_ROW_COUNT;
    private final int PARTITION_THRESHOLD_HIER_ROW_COUNT;

    private final String ORACLE_DRIVER;
    private final String H2_DRIVER;
    private final String SQLSERVER_DRIVER;
    private final String POSTGRES_DRIVER;
    private final String SAP_DRIVER;

    private SQLGenerator sqlGenerator;
    private List<String> partitionedHierchiesList;

    public RollupMaker(CubeSpecification cube) {
        logger = LoggerFactory.getLogger(RollupMaker.class);
        this.CUBE_SPEC = cube;
        this.partitionedHierchiesList = new ArrayList<>();

        ResourceBundle rb = ResourceBundle.getBundle("dbsettings");

        PARTITION_THRESHOLD_ALL_TABLE_ROW_COUNT = Integer.parseInt(rb.getString("partitionThresholdAllTableRowCount"));
        PARTITION_THRESHOLD_HIER_ROW_COUNT = Integer.parseInt(rb.getString("partitionThresholdHierRowCount"));

        ORACLE_DRIVER = rb.getString("oracleDriver");
        H2_DRIVER = rb.getString("h2Driver");
        SQLSERVER_DRIVER = rb.getString("sqlServerDriver");
        POSTGRES_DRIVER = rb.getString("postgreSqlDriver");
        SAP_DRIVER = rb.getString("sapDriver");

        if (cube.getSourceDBDriver().equals(ORACLE_DRIVER)) {
            this.sqlGenerator = new OracleSQLGenerator();
        } else if (cube.getSourceDBDriver().equals(H2_DRIVER)) {
            this.sqlGenerator = new H2SQLGenerator();
        } else if (cube.getSourceDBDriver().equals(SQLSERVER_DRIVER)) {
            this.sqlGenerator = new SQLServerSQLGenerator();
        } else if (cube.getSourceDBDriver().equals(POSTGRES_DRIVER)) {
            this.sqlGenerator = new PostgreSQLGenerator();
        } else if (cube.getSourceDBDriver().equals(H2_DRIVER)) {
            this.sqlGenerator = new SAPSQLGenerator();
        }

    }

    private void makeAllHierarchiesRollupInDestinationTable(String destinationTableName, String sourceTableName) throws SQLException, ClassNotFoundException {
        String dbUser = CUBE_SPEC.getSourceDBUser();
        String dbPassword = CUBE_SPEC.getSourceDBPassword();
        String dbUrl = CUBE_SPEC.getSourceDBURL();
        String dbDriver = CUBE_SPEC.getSourceDBDriver();

        for (String dimName : partitionedHierchiesList) {

            logger.info("## " + dimName + " processing...");

            String sql = this.sqlGenerator.getDropSQL("TMP_", destinationTableName);
            logger.debug(sql);
            DBService.slientExecuteQuery(sql, CUBE_SPEC.getSourceDBUser(), CUBE_SPEC.getSourceDBPassword(), CUBE_SPEC.getSourceDBURL(), CUBE_SPEC.getSourceDBDriver());

            sql = this.sqlGenerator.getCreateSQL("TMP_", CUBE_SPEC, destinationTableName);
            logger.debug(sql);
            DBService.executeQuery(sql, dbUser, dbPassword, dbUrl, dbDriver);

            List<LevelSpecification> levels = new ArrayList<>();

            sql = this.sqlGenerator.getDropSQL("TMP_", destinationTableName);
            logger.debug(sql);
            DBService.slientExecuteQuery(sql, dbUser, dbPassword, dbUrl, dbDriver);

            sql = this.sqlGenerator.getCreateSQL("TMP_", CUBE_SPEC, destinationTableName);
            logger.debug(sql);
            DBService.executeQuery(sql, dbUser, dbPassword, dbUrl, dbDriver);

            makeOneHierRollupInTmpTable(dimName,
                    CUBE_SPEC.getDimensionByName(dimName), destinationTableName);

            sql = this.sqlGenerator.getSelectQueryToLoadOneHierarchyRollup("TMP_", CUBE_SPEC, destinationTableName, sourceTableName);
            DBService.executeQuery(sql, dbUser, dbPassword, dbUrl, dbDriver);
            logger.debug(sql);

            sql = this.sqlGenerator.getDropSQL("TMP_", destinationTableName);
            DBService.slientExecuteQuery(sql, dbUser, dbPassword, dbUrl, dbDriver);
            logger.debug(sql);
        }

    }

    private void makeOneHierRollupInTmpTable(String dimName, DimensionSpecification hier, String destinationTableName) throws SQLException, ClassNotFoundException {
        List<String> hierColumnList = hier.getColumnListToOLAPBuilding();
        List<String> allDimensionColumnListMinusHierarchyColumnList = CUBE_SPEC.getDistinctDimensionColumnList();
        allDimensionColumnListMinusHierarchyColumnList.removeAll(hierColumnList);
        List<String> measureColumnList = CUBE_SPEC.getDistinctMeasureColumnList();

        if (hier != null) {
            for (int i = 0; i < hier.getOlapSqlIterationCnt(); i++) {
                StringBuilder offlineCalculetedSQL = new StringBuilder("SELECT ");
                String selectStatement = hier.getOlapSelectStatementByIteration(i);
                offlineCalculetedSQL.append(selectStatement).append(", ");
                for (String otherDimensionColumnName : allDimensionColumnListMinusHierarchyColumnList) {
                    offlineCalculetedSQL.append(otherDimensionColumnName).append(", ");
                }

                String toInsertStatement = offlineCalculetedSQL.substring(0, offlineCalculetedSQL.length() - 2);

                for (String measureColumnName : measureColumnList) {
                    String aggFunction = CUBE_SPEC.getAggregationFunctionByName(hier.getUniqueName(), measureColumnName);
                    offlineCalculetedSQL.append(aggFunction).append("(").append(measureColumnName).append("), ");
                }
                offlineCalculetedSQL = new StringBuilder(offlineCalculetedSQL.substring(0, offlineCalculetedSQL.length() - 2));
                offlineCalculetedSQL.append(" FROM ").append(destinationTableName).append(" ");
                offlineCalculetedSQL.append(" GROUP BY ");

                String groupByStatement = hier.getOlapGroupByStatementByIteration(i);
                offlineCalculetedSQL.append(groupByStatement);

                if (groupByStatement != null && !groupByStatement.isEmpty()) {
                    offlineCalculetedSQL.append(", ");
                }

                for (String otherDimensionColumnName : allDimensionColumnListMinusHierarchyColumnList) {
                    offlineCalculetedSQL.append(otherDimensionColumnName).append(", ");
                }

                offlineCalculetedSQL = new StringBuilder(offlineCalculetedSQL.substring(0, offlineCalculetedSQL.length() - 2));

                String sql = getInsertStatementFromSeletcStatement(toInsertStatement, destinationTableName) + offlineCalculetedSQL.toString();
                logger.debug(sql);
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

    public void make(String destinationTableName, String sourceTableName) throws SQLException, ClassNotFoundException {
        this.partitionedHierchiesList = CUBE_SPEC.getIsOfflineCalculatedDimensonList();
        logger.info("Start");

        init(destinationTableName, sourceTableName);
        loadBaseLevel(destinationTableName, sourceTableName);
        makeAllHierarchiesRollupInDestinationTable(destinationTableName, sourceTableName);

    }

    private void init(String destinationTableName, String sourceTableName) throws SQLException, ClassNotFoundException {
        String sql = this.sqlGenerator.getDropSQL(null, destinationTableName);
        logger.debug(sql);
        DBService.slientExecuteQuery(sql, CUBE_SPEC.getSourceDBUser(), CUBE_SPEC.getSourceDBPassword(), CUBE_SPEC.getSourceDBURL(), CUBE_SPEC.getSourceDBDriver());

        sql = this.sqlGenerator.getCreateSQL(null, CUBE_SPEC, destinationTableName);
        logger.debug(sql);
        DBService.executeQuery(sql, CUBE_SPEC.getSourceDBUser(), CUBE_SPEC.getSourceDBPassword(), CUBE_SPEC.getSourceDBURL(), CUBE_SPEC.getSourceDBDriver());

    }

    private void loadBaseLevel(String destinationTableName, String sourceTableName) throws SQLException, ClassNotFoundException {
        String sql = this.sqlGenerator.getSQLToBaseLevelDataLoading(null, CUBE_SPEC, destinationTableName, sourceTableName);
        logger.debug(sql);
        DBService.executeQuery(sql, CUBE_SPEC.getSourceDBUser(), CUBE_SPEC.getSourceDBPassword(), CUBE_SPEC.getSourceDBURL(), CUBE_SPEC.getSourceDBDriver());
    }
}
