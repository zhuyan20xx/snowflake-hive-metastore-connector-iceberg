/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.core.SnowflakeClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;

import javax.sql.RowSet;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

public class TestUtil
{
  /**
   * Helper class to initialize the Hive metastore handler, which is commonly
   * used for tests in test classes.
   */
  public static IHMSHandler initializeMockHMSHandler()
  {
    // Mock the HMSHandler and configurations
    Configuration mockConfig = PowerMockito.mock(Configuration.class);
    HiveMetaStore.HMSHandler mockHandler =
        PowerMockito.mock(HiveMetaStore.HMSHandler.class);
    PowerMockito.when(mockConfig.get("fs.s3n.awsAccessKeyId"))
        .thenReturn("accessKeyId");
    PowerMockito.when(mockConfig.get("fs.s3n.awsSecretAccessKey"))
        .thenReturn("awsSecretKey");
    PowerMockito.when(mockConfig.get("snowflake.jdbc.schema"))
        .thenReturn("someSchema");
    PowerMockito.when(mockConfig.getStringCollection("snowflake.hive-metastore-listener.schemas"))
          .thenReturn(Arrays.asList(new String[]{"someDb", "someSchema2", "DB1"}));
    PowerMockito.when(mockHandler.getConf()).thenReturn(mockConfig);

    return mockHandler;
  }

  /**
   * Helper method to initialize the SnowflakeConf configuration class,
   * which is commonly used for tests in test classes.
   */
  public static SnowflakeConf initializeMockConfig()
  {
    SnowflakeConf mockConfig = PowerMockito.mock(SnowflakeConf.class);
    PowerMockito
        .when(mockConfig.get("snowflake.jdbc.db", null))
        .thenReturn("someDB");
    PowerMockito
        .when(mockConfig.get("snowflake.jdbc.schema"))
        .thenReturn("someSchema1");
    PowerMockito
        .when(mockConfig.getBoolean("snowflake.hive-metastore-listener.enable-creds-from-conf", false))
        .thenReturn(true);
    PowerMockito
        .when(mockConfig.getInt("snowflake.hive-metastore-listener.retry.timeout", 1000))
        .thenReturn(0);
    PowerMockito
        .when(mockConfig.getInt("snowflake.hive-metastore-listener.retry.count", 3))
        .thenReturn(3);
    PowerMockito
        .when(mockConfig.getInt("snowflake.hive-metastore-listener.client-thread-count", 8))
        .thenReturn(1);
    PowerMockito
        .when(mockConfig.getBoolean("snowflake.hive-metastore-listener.force-synchronous", false))
        .thenReturn(true);
    PowerMockito
        .when(mockConfig.get("snowflake.hive-metastore-listener.data-column-casing", "NONE"))
        .thenReturn("NONE");
    PowerMockito
        .when(mockConfig.getStringCollection("snowflake.hive-metastore-listener.schemas"))
        .thenReturn(Arrays.asList(new String[]{"someDb", "someSchema2", "DB1"}));
    return mockConfig;
  }

  /**
   * Helper method to initialize a base Table object for tests
   */
  public static Table initializeMockTable()
  {
    Table table = new Table();

    table.setTableName("t1");
    table.setDbName("someDB");
    table.setPartitionKeys(Arrays.asList(
        new FieldSchema("partcol", "int", null),
        new FieldSchema("name", "string", null)));
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(new ArrayList<>());
    table.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    table.getSd().setLocation("s3n://bucketname/path/to/table");
    table.getSd().setSerdeInfo(new SerDeInfo());
    table.getSd().getSerdeInfo().setSerializationLib(
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    table.getSd().getSerdeInfo().setParameters(new HashMap<>());
    table.setParameters(new HashMap<>());

    return table;
  }

  public static Table initializeMockIcebergRefreshTable()
  {
    Table table = new Table();

    table.setTableName("test_iceberg_tb");
    table.setDbName("icebergdemo");
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(new ArrayList<>());
    table.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    table.getSd().setLocation("s3n://bucketname/path/to/table");
    table.getSd().setSerdeInfo(new SerDeInfo());
    table.getSd().getSerdeInfo().setSerializationLib(
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    table.getSd().getSerdeInfo().setParameters(new HashMap<>());
    HashMap<String,String> parameters = new HashMap<>();
    parameters.put("metadata_location","S3://iubeta/apps/hive/warehouse/jpz3928.db/task_schema_change_test5/metadata/00004-9df00026-c7fe-48e4-ab07-9c7c06aad3ea.metadata.json");
    table.setParameters(parameters);

    return table;
  }

  public static Table initializeMockIcebergCreateTable()
  {
    Table table = new Table();

    table.setTableName("test_iceberg_tb");
    table.setDbName("icebergdemo");
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(new ArrayList<>());
    table.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    table.getSd().setLocation("s3n://bucketname/path/to/table");
    table.getSd().setSerdeInfo(new SerDeInfo());
    table.getSd().getSerdeInfo().setSerializationLib(
            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    table.getSd().getSerdeInfo().setParameters(new HashMap<>());
    HashMap<String,String> parameters = new HashMap<>();
    parameters.put("CATALOG","zzengIcebergCatalogInt");
    parameters.put("EXTERNAL_VOLUME","extIcebergVolC");
    parameters.put("BASE_LOCATION","airlines/airlines");
    parameters.put("metadata_location","S3://iubeta/apps/hive/warehouse/jpz3928.db/task_schema_change_test5/metadata/00005-07601ab7-6eac-4562-a87d-1dd3a5bd05d5.metadata.json");
    table.setParameters(parameters);

    return table;
  }

  /**
   * Helper method to mock the Snowflake client to return the provided stage
   * location when querying Snowflake with a stage
   * @param stageLocation The location that should be returned by the Snowflake
   *                      client.
   */
  public static void mockSnowflakeStageWithLocation(String stageLocation, String expectedSchema)
      throws Exception
  {
    ResultSetMetaData mockMetadata = PowerMockito.mock(ResultSetMetaData.class);
    PowerMockito.when(mockMetadata.getColumnCount()).thenReturn(3);
    PowerMockito.when(mockMetadata.getColumnName(1)).thenReturn("something");
    PowerMockito.when(mockMetadata.getColumnName(2)).thenReturn("url");
    PowerMockito.when(mockMetadata.getColumnName(3)).thenReturn("something2");
    RowSet mockRowSet = PowerMockito.mock(RowSet.class);
    PowerMockito
        .when(mockRowSet.next())
        .thenReturn(true)
        .thenReturn(false);
    PowerMockito
        .when(mockRowSet.getString(2))
        .thenReturn(stageLocation);
    PowerMockito.when(mockRowSet.getMetaData()).thenReturn(mockMetadata);
    PowerMockito.mockStatic(SnowflakeClient.class);
    PowerMockito // Note: clobbers mocks for SnowflakeClient.executeStatement
        .when(SnowflakeClient.executeStatement(any(Connection.class),
                                               anyString(),
                                               any(SnowflakeConf.class)))
        .thenReturn(mockRowSet);
    PowerMockito
            .when(SnowflakeClient.getConnection(any(SnowflakeConf.class), Matchers.eq(expectedSchema)))
            .thenReturn(PowerMockito.mock(SnowflakeConnectionV1.class));
  }
}
