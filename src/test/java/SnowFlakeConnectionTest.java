import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SnowFlakeConnectionTest {

    public static void main(String[] args) throws Exception {
        // get connection
        System.out.println("Create JDBC connection");
        Connection connection = getConnection();
        System.out.println("Done creating JDBC connectionn");
        // create statement
        System.out.println("Create JDBC statement");
        Statement statement = connection.createStatement();
        System.out.println("Done creating JDBC statementn");
        // create a table
        System.out.println("Create demo table");
        statement.executeUpdate("ALTER ICEBERG TABLE test_iceberg_tb REFRESH 'metadata/00004-9df00026-c7fe-48e4-ab07-9c7c06aad3ea.metadata.json';");
        statement.close();

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
