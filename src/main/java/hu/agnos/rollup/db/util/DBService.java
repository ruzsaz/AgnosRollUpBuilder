package hu.agnos.rollup.db.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author michaelcote
 */
public class DBService {

    private static final Logger logger = LoggerFactory.getLogger(DBService.class);

    public static Connection createDatabaseConnection(String user, String password, String url, String driver) throws SQLException, ClassNotFoundException {
        Class.forName(driver);
        Connection c = DriverManager.getConnection(url, user, password);
        return c;
    }

    public static boolean executeQuery(String sql, String user, String password, String url, String driver) throws SQLException, ClassNotFoundException {
        boolean result = true;
        Connection connection = null;
        Statement stmt = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
            stmt.execute(sql);
            connection.commit();
   
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    logger.error(ex.getMessage());
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    logger.error(ex.getMessage());
                }
            }
        }
        return result;
    }

    public static void slientExecuteQuery(String sql, String user, String password, String url, String driver) {
        Connection connection = null;
        Statement stmt = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
            stmt.execute(sql);
            connection.commit();
        } catch (ClassNotFoundException | SQLException ex) {
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                }
            }
        }
    }
}