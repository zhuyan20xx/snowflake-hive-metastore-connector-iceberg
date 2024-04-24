import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.commands.AlterIcebergTable;
import net.snowflake.hivemetastoreconnector.core.SnowflakeClient;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class AlertIcebergTableTest {

    public static void main(String[] args) throws Exception {
//        testCreateTable();
        testRefreshTable();
    }

    private static void testRefreshTable() throws Exception{
        Table table = TestUtil.initializeMockIcebergRefreshTable();
        IHMSHandler mockHandler = TestUtil.initializeMockHMSHandler();
        AlterTableEvent alterTableEvent = new AlterTableEvent(table, table,
                true, true, mockHandler);

        AlterIcebergTable alterIcebergTable = new AlterIcebergTable(alterTableEvent, TestUtil.initializeMockConfig());
        System.out.println(alterIcebergTable.generateSqlQueries());
        SnowflakeClient.generateAndExecuteSnowflakeStatements(alterIcebergTable, new SnowflakeConf());
        System.out.println("OK");
    }

    private static void testCreateTable() throws Exception{
        Table table = TestUtil.initializeMockIcebergCreateTable();
        IHMSHandler mockHandler = TestUtil.initializeMockHMSHandler();
        AlterTableEvent alterTableEvent = new AlterTableEvent(table, table,
                true, true, mockHandler);

        AlterIcebergTable alterIcebergTable = new AlterIcebergTable(alterTableEvent, TestUtil.initializeMockConfig());
        System.out.println(alterIcebergTable.generateSqlQueries());
        SnowflakeClient.generateAndExecuteSnowflakeStatements(alterIcebergTable, new SnowflakeConf());
        System.out.println("OK");
    }
    private static Connection getConnection()
            throws SQLException {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
        } catch (ClassNotFoundException ex) {
            System.err.println("Driver not found");
        }
        // build connection properties
        Properties properties = new Properties();
        properties.put("user", "shinjisone");     // replace "" with your username
        properties.put("password", "XuexiSnow2024"); // replace "" with your password
        properties.put("account", "nd88088");  // replace "" with your account name
        properties.put("db", "ZZENG");       // replace "" with target database name
        properties.put("schema", "icebergdemo");   // replace "" with target schema name
        //properties.put("tracing", "on");

        // create a new connection
        String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
        // use the default connection string if it is not set in environment
        if (connectStr == null) {
            connectStr = "jdbc:snowflake://drndyyc-nd88088.snowflakecomputing.com"; // replace accountName with your account name
        }
        return DriverManager.getConnection(connectStr, properties);
    }
}
