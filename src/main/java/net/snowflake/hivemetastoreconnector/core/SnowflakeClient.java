/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.core;

import com.google.common.base.Preconditions;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.SnowflakeIcebergListener;
import net.snowflake.hivemetastoreconnector.commands.Command;
import net.snowflake.client.jdbc.internal.apache.commons.codec.binary.Base64;
import net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider;
import net.snowflake.hivemetastoreconnector.util.HiveToSnowflakeSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.ql.secrets.SecretSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * Class that uses the snowflake jdbc to connect to snowflake.
 * Executes the commands
 * TODO: modify exception handling
 */
public class SnowflakeClient
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeIcebergListener.class);
  private static Scheduler scheduler;

  /**
   * Creates and executes an event of Iceberg Table for snowflake. Events may be processed in
   * the background, but events on the same table will be processed in order.
   * @param event - the hive event details
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   */
  public static void createAndExecuteCommandIcebergForSnowflake(
          ListenerEvent event,
          SnowflakeConf snowflakeConf) throws MetaException
  {
    Preconditions.checkNotNull(event);

    // Obtains the proper command
    log.info("Creating the Snowflake command");
    Command command = CommandGenerator.getIcebergCommand(event, snowflakeConf);

    boolean backgroundTaskEnabled = !snowflakeConf.getBoolean(
            SnowflakeConf.ConfVars.SNOWFLAKE_CLIENT_FORCE_SYNCHRONOUS.getVarname(), false);
    if (backgroundTaskEnabled)
    {
      initScheduler(snowflakeConf);
      scheduler.enqueueMessage(command);
    }
    else
    {
      try {
        generateAndExecuteSnowflakeStatements(command, snowflakeConf);
      }catch (Exception e){
        throw new MetaException(e.getMessage());
      }
    }
  }

  /**
   * Helper method. Generates commands for an event and executes those commands.
   * Synchronous.
   * @param command - the command to generate statements from
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   */
  public static void generateAndExecuteSnowflakeStatements(
      Command command,
      SnowflakeConf snowflakeConf) throws Exception
  {
    // Generate the string queries for the command
    // Some Hive commands require more than one statement in Snowflake
    // For example, for create table, a stage must be created before the table
    log.info("Generating Snowflake queries");
    List<String> commandList;

    String schema =
          HiveToSnowflakeSchema.getSnowflakeSchemaFromHiveSchema(
              command.getDatabaseName(),
              snowflakeConf);
    commandList = command.generateSqlQueries();
    executeStatements(commandList, snowflakeConf, schema);


  }

  /**
   * Helper method to connect to Snowflake and execute a list of queries
   * @param commandList - The list of queries to execute
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   * @param schema - the schema to use for the jdbc connection
   */
  public static void executeStatements(List<String> commandList,
                                       SnowflakeConf snowflakeConf,
                                       String schema) throws MetaException
  {
    log.info("Executing statements: " + String.join(", ", commandList));

    // Get connection
    log.info("Getting connection to the Snowflake");
    try (Connection connection = retry(
        () -> getConnection(snowflakeConf, schema), snowflakeConf))
    {
      commandList.forEach(commandStr ->
      {
        try (Statement statement =
            retry(connection::createStatement, snowflakeConf))
        {
          log.info("Executing command: " + commandStr);
          ResultSet resultSet = retry(
              () -> statement.executeQuery(commandStr), snowflakeConf);
          StringBuilder sb = new StringBuilder();
          sb.append("Result:\n");
          while (resultSet.next())
          {
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++)
            {
              if (i == resultSet.getMetaData().getColumnCount())
              {
                sb.append(resultSet.getString(i));
              }
              else
              {
                sb.append(resultSet.getString(i));
                sb.append("|");
              }
            }
            sb.append("\n");
          }
          log.info(sb.toString());
        }
        catch (Exception e)
        {
          log.error("There was an error executing the statement: " +
                        e.getMessage());
          throw new RuntimeException(e);
        }
      });
    }
    catch (Exception e){
      log.error("There was an error creating the query: " +
              e.toString());
      //for debug
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String secretName = snowflakeConf.get(SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_SECRETURL.getVarname());
      throw new MetaException(secretName+"  "+sw.toString());
    }
  }

  /**
   * (Deprecated)
   * Utility method to connect to Snowflake and execute a query.
   * @param connection - The connection to use
   * @param commandStr - The query to execute
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   * @return The result of the executed query
   * @throws SQLException Thrown if there was an error executing the
   *                      statement or forming a connection.
   */
  public static ResultSet executeStatement(Connection connection,
                                           String commandStr,
                                           SnowflakeConf snowflakeConf)
      throws SQLException
  {
    Statement statement = retry(connection::createStatement, snowflakeConf);
    log.info("Executing command: " + commandStr);
    ResultSet resultSet = retry(() -> statement.executeQuery(commandStr),
                                snowflakeConf);
    log.info("Command successfully executed");
    return resultSet;
  }

  /**
   * Helper method. Initializes and starts the query scheduler
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  private static void initScheduler(SnowflakeConf snowflakeConf)
  {
    if (scheduler != null)
    {
      return;
    }

    int numThreads = snowflakeConf.getInt(
        SnowflakeConf.ConfVars.SNOWFLAKE_CLIENT_THREAD_COUNT.getVarname(), 8);

    scheduler = new Scheduler(numThreads, snowflakeConf);
  }

  /**
   * Get the connection to the Snowflake account.
   * First finds a Snowflake driver and connects to Snowflake using the
   * given properties.
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   * @param schema - the schema to use for the connection
   * @return The JDBC connection
   * @throws SQLException Exception thrown when initializing the connection
   */
  public static Connection getConnection(SnowflakeConf snowflakeConf, String schema)
      throws Exception
  {
    try
    {
      Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
    }
    catch(ClassNotFoundException e)
    {
      log.error("Driver not found");
    }

    // build connection properties
    Properties properties = new Properties();

    snowflakeConf.forEach(conf ->
      {
        if (!conf.getKey().startsWith("snowflake.jdbc"))
        {
          return;
        }

        SnowflakeConf.ConfVars confVar =
            SnowflakeConf.ConfVars.findByName(conf.getKey());
        if (!confVar.isSnowflakeJDBCProperty())
        {
          return;
        }

        properties.put(confVar.getSnowflakePropertyName(), conf.getValue());
      });

    // JDBC password
    String snowflakePassword = snowflakeConf.getSecret(
        SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_PASSWORD.getVarname());
    if (snowflakePassword != null)
    {
      properties.put(SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_PASSWORD.getSnowflakePropertyName(),
                     snowflakePassword);
    }else{
      properties.put(SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_PASSWORD.getSnowflakePropertyName(),
              getJDBCPasswordFromSecretSource(snowflakeConf));
    }

    // JDBC private key
    String privateKeyConf = snowflakeConf.getSecret(
        SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_PRIVATE_KEY.getVarname());
    if (privateKeyConf != null)
    {
      try
      {
        Security.addProvider(new BouncyCastleProvider());
        byte[] keyBytes = Base64.decodeBase64(privateKeyConf);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
        properties.put("privateKey", keyFactory.generatePrivate(keySpec));
      }
      catch (InvalidKeySpecException | NoSuchAlgorithmException e)
      {
        throw new IllegalArgumentException(
            String.format("Private key is invalid: %s", e), e);
      }
    }

    properties.put(SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_SCHEMA.getSnowflakePropertyName(), schema);

    String connectStr = snowflakeConf.get(
        SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_CONNECTION.getVarname());
    log.info(properties.toString());
    return DriverManager.getConnection(connectStr, properties);
  }

  private static String getJDBCPasswordFromSecretSource(SnowflakeConf snowflakeConf) throws Exception{
    String secretSourceClass =  snowflakeConf.get(SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_SECRETSOURCE.getVarname(),
            "org.apache.hadoop.hive.ql.secrets.AWSSecretsManagerSecretSource");
    SecretSource source = (SecretSource) Class.forName(secretSourceClass).newInstance();
    URI secretName = new URI(snowflakeConf.get(SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_SECRETURL.getVarname()));
    return source.getSecret(secretName);
  }

  /**
   * Helper interface that represents a Supplier that can throw an exception.
   * @param <T> The type of object returned by the supplier
   * @param <E> The type of exception thrown by the supplier
   */
  @FunctionalInterface
  public interface ThrowableSupplier<T, E extends Throwable>
  {
    T get() throws E;
  }

  /**
   * Helper method for simple retries.
   * Note: The total number of attempts is 1 + retries.
   * @param <T> The type of object returned by the supplier
   * @param <E> The type of exception thrown by the supplier
   * @param method The method to be executed and retried on.
   * @param maxRetries The maximum number of retries.
   * @param timeoutInMilliseconds Time between retries.
   */
  private static <T, E extends Throwable> T retry(
      ThrowableSupplier<T,E> method,
      int maxRetries,
      int timeoutInMilliseconds)
  throws E
  {
    // Attempt to call the method with N-1 retries
    for (int i = 0; i < maxRetries; i++)
    {
      try
      {
        // Attempt to call the method
        return method.get();
      }
      catch (Exception e)
      {
        // Wait between retries
        try
        {
          Thread.sleep(timeoutInMilliseconds);
        }
        catch (InterruptedException interruptedEx)
        {
          log.error("Thread interrupted.");
          Thread.currentThread().interrupt();
        }
      }
    }

    // Retry one last time, the exception will by handled by the caller
    return method.get();
  }

  /**
   * Helper method for simple retries. Overload for default arguments.
   * @param <T> The type of object returned by the supplier
   * @param <E> The type of exception thrown by the supplier
   * @param method The method to be executed and retried on.
   * @param snowflakeConf The snowflake configuration to use.
   *
   * @return The result of the method.
   */
  public static <T, E extends Throwable> T retry(
      ThrowableSupplier<T, E> method,
      SnowflakeConf snowflakeConf)
  throws E
  {
    int maxRetries = snowflakeConf.getInt(
        SnowflakeConf.ConfVars.SNOWFLAKE_HIVEMETASTORELISTENER_RETRY_COUNT.getVarname(), 1);
    int timeoutInMilliseconds = snowflakeConf.getInt(
        SnowflakeConf.ConfVars.SNOWFLAKE_HIVEMETASTORELISTENER_RETRY_TIMEOUT_MILLISECONDS.getVarname(), 1000);
    return retry(method, maxRetries, timeoutInMilliseconds);
  }
}
