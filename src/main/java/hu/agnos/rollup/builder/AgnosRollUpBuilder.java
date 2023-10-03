/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hu.agnos.rollup.builder;

import hu.agnos.cube.specification.entity.CubeSpecification;
import hu.agnos.rollup.controller.RollupMaker;
import hu.agnos.cube.specification.exception.InvalidPostfixExpressionException;
import hu.agnos.cube.specification.exception.NameOfHierarchySpecificationNotUniqueException;
import hu.agnos.cube.specification.exception.NameOfMeasureSpecificationNotUniqueException;
import hu.agnos.cube.specification.repo.CubeSpecificationRepo;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author parisek
 */
public class AgnosRollUpBuilder {

    private static String xmlPath = null;

    private static String inputTable = null;

    private static String outputTable = null;

    private static final Logger logger = LoggerFactory.getLogger(AgnosRollUpBuilder.class);

    ;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws NameOfHierarchySpecificationNotUniqueException, NameOfMeasureSpecificationNotUniqueException, IOException, InvalidPostfixExpressionException {
        int result = 0;

        String badParamsStg = "Params:\n"
                + "\t--xml: path of xml file (mandatory)\n"
                + "\t--i: name of input table (mandatory)\n"
                + "\t--o:  name of output table (mandatory\n";

        if (args.length != 3) {
            logger.error(badParamsStg);
            result = 1;
        } else {
            parseArgs(args);

            if (xmlPath == null
                    || inputTable == null
                    || outputTable == null) {
                System.out.println(badParamsStg);
                result = 1;
            } else {
//                System.out.println("agnos rollup started on \"" + inputTable +"\" at " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()));

                CubeSpecification cube = getCubeSpecification(xmlPath);
//                System.out.println("ez van az xmlben: " + xmlCube.getSourceDBURL());
                if (cube != null) {
                    try {
                        build(cube, inputTable, outputTable);
                    } catch (SQLException | ClassNotFoundException ex) {
                        logger.error(ex.getMessage());
                        result = 1;
                    }
                } else {
                    result = 1;
                }
            }
            if (result == 0) {
                logger.info("Make rollup from "+ xmlPath + " file was successful." );
            } else {
                logger.info("Make rollup from " + xmlPath + " file was unsuccessful.");
            }
            System.exit(result);
        }
    }

    private static void build(CubeSpecification rollup, String sourceTableName, String destinationTableName) throws SQLException, ClassNotFoundException {
        RollupMaker rolapMaker = new RollupMaker(rollup);
        rolapMaker.make(destinationTableName, sourceTableName);

    }

    private static void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--xml=")) {
                xmlPath = args[i].substring(6);
            } else if (args[i].startsWith("--i=")) {
                inputTable = args[i].substring(4);
            } else if (args[i].startsWith("--o=")) {
                outputTable = args[i].substring(4);
            }
        }
    }

    private static CubeSpecification getCubeSpecification(String path) throws NameOfHierarchySpecificationNotUniqueException, NameOfMeasureSpecificationNotUniqueException, IOException, InvalidPostfixExpressionException {
        CubeSpecificationRepo cubeRepo = new CubeSpecificationRepo();
        return cubeRepo.findCubeSpecificationByPath(path);
    }
}
