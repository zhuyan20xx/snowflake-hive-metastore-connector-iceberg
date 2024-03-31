/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.commands;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.SnowflakeIcebergListener;
import net.snowflake.hivemetastoreconnector.util.HiveToSnowflakeType;
import net.snowflake.hivemetastoreconnector.util.IcebergTableUtil;
import net.snowflake.hivemetastoreconnector.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * A class for the AlterExternalTable command
 * TODO: Split into TouchExternalTable and AlterColumnsExternalTable
 * @author wwong
 */
public class AlterIcebergTable extends Command
{

  private static final Logger log =
          LoggerFactory.getLogger(SnowflakeIcebergListener.class);

  /**
   * Creates a AlterTable command
   * @param alterTableEvent Event to generate a command from
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public AlterIcebergTable(AlterTableEvent alterTableEvent,
                           SnowflakeConf snowflakeConf)
  {
    super(Preconditions.checkNotNull(alterTableEvent).getOldTable());
    this.oldHiveTable =
        Preconditions.checkNotNull(alterTableEvent.getOldTable());
    this.newHiveTable =
        Preconditions.checkNotNull(alterTableEvent.getNewTable());
    this.snowflakeConf = Preconditions.checkNotNull(snowflakeConf);
    this.hiveConf = Preconditions.checkNotNull(
        alterTableEvent.getHandler().getConf());
  }

  /**
   * Generates the necessary queries on a Hive alter table event
   * @return The Snowflake queries generated
   * @throws SQLException Thrown when there was an error executing a Snowflake
   *                      SQL query (if a Snowflake query must be executed).
   * @throws UnsupportedOperationException Thrown when the input is invalid
   */
  public List<String> generateSqlQueries()
      throws SQLException, UnsupportedOperationException
  {
    List<String> commands = new ArrayList<>();
    if(IcebergTableUtil.isAbletoCreateTable(newHiveTable)){
      commands = new CreateIcebergTable(
              newHiveTable,
              snowflakeConf,
              hiveConf,
              false // Do not replace table
      ).generateSqlQueries();
    }else {
      // ALTER ICEBERG TABLE my_iceberg_table REFRESH 'metadata/v1.metadata.json';
      String refreshCommand = String.format("ALTER ICEBERG TABLE %s REFRESH '%s';",
              StringUtil.escapeSqlIdentifier(newHiveTable.getTableName()),
              IcebergTableUtil.getMetadataLocation(newHiveTable));
      commands.add(refreshCommand);
    }
    return commands;
  }

  private final Table oldHiveTable;

  private final Table newHiveTable;

  private final Configuration hiveConf;

  private final SnowflakeConf snowflakeConf;
}
