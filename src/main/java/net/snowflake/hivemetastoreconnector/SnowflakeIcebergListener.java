/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector;

import com.google.common.base.Preconditions;
import net.snowflake.hivemetastoreconnector.core.SnowflakeClient;
import net.snowflake.hivemetastoreconnector.util.IcebergTableUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * The listener for Hive
 * This listener will get notified when the corresponding command is called
 */
public class SnowflakeIcebergListener extends MetaStoreEventListener
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeIcebergListener.class);

  private static SnowflakeConf snowflakeConf;

  private static Pattern tableNameFilter; // null if there is no filter

  private static Pattern databaseNameFilter; // null if there is no filter

  public SnowflakeIcebergListener(Configuration config)
  {
    super(config);

    // generate the snowflake jdbc conf
    snowflakeConf = new SnowflakeConf();
    tableNameFilter = snowflakeConf.getPattern(
        SnowflakeConf.ConfVars.SNOWFLAKE_TABLE_FILTER_REGEX.getVarname(), null);
    databaseNameFilter = snowflakeConf.getPattern(
        SnowflakeConf.ConfVars.SNOWFLAKE_DATABASE_FILTER_REGEX.getVarname(), null);
    log.info("SnowflakeIcebergListener created");
  }

  /**
   * The listener for the create table command
   * @param tableEvent An event that was listened for
   */
//  @Override
//  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException
//  {
//    logTableEvent("Event received", tableEvent, tableEvent.getTable());
//    if (shouldHandleCreateTable(tableEvent, tableEvent.getTable()))
//    {
//      SnowflakeClient.createAndExecuteCommandIcebergForSnowflake(tableEvent,
//                                                          snowflakeConf);
//    }
//    else
//    {
//      logTableEvent("Nothing to do", tableEvent, tableEvent.getTable());
//    }
//  }

  /**
   * The listener for the drop table command
   * @param tableEvent An event that was listened for
   */
//  @Override
//  public void onDropTable(DropTableEvent tableEvent)
//  {
//    logTableEvent("Event received", tableEvent, tableEvent.getTable());
//    if (shouldHandle(tableEvent, tableEvent.getTable()))
//    {
//      SnowflakeClient.createAndExecuteCommandIcebergForSnowflake(tableEvent,
//                                                          snowflakeConf);
//    }
//    else
//    {
//      logTableEvent("Nothing to do", tableEvent, tableEvent.getTable());
//    }
//  }

  /**
   * The listener for the alter table command
   * @param tableEvent An event that was listened for
   */
  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException
  {
    logTableEvent("Event received", tableEvent, tableEvent.getNewTable());
    if (shouldHandle(tableEvent, tableEvent.getNewTable()))
    {
      SnowflakeClient.createAndExecuteCommandIcebergForSnowflake(tableEvent,
                                                          snowflakeConf);
    }
    else
    {
      logTableEvent("Nothing to do", tableEvent, tableEvent.getNewTable());
    }
  }


  /**
   * Helper method for logging a message related to an event and Hive table
   * @param message The message to log
   * @param event The event
   * @param hiveTable The Hive table associated with the event
   */
  private static void logTableEvent(String message, ListenerEvent event,
                                    Table hiveTable)
  {
    Preconditions.checkNotNull(message);
    Preconditions.checkNotNull(event);
    Preconditions.checkNotNull(hiveTable);
    String tableName = Preconditions.checkNotNull(hiveTable.getTableName());
    log.info(String.format("SnowflakeIcebergListener: %s (Event='%s' Table='%s')",
                           message,
                           event.getClass().getSimpleName(),
                           tableName));
  }

  /**
   * Helper method to determine whether the listener should handle an creata table event
   * @param event The event
   * @param table The Hive table associated with the event
   * @return True if the event should be handled, false otherwise
   */
  private static boolean shouldHandleCreateTable(ListenerEvent event, Table table)
  {
    if (!shouldHandle(event, table)){
      return false;
    }
    if(!IcebergTableUtil.isAbletoCreateTable(table))
    {
      logTableEvent("Skip event, missing sf_catalog or sf_external_volume or sf_base_location property of iceberg table",
              event, table);
      return false;
    }
    return true;
  }

  /**
   * Helper method to determine whether the listener should handle an common event
   * @param event The event
   * @param table The Hive table associated with the event
   * @return True if the event should be handled, false otherwise
   */
  private static boolean shouldHandle(ListenerEvent event, Table table)
  {
    if (!event.getStatus())
    {
      logTableEvent("Skip event, as status is false", event, table);
      return false;
    }

    if (tableNameFilter != null && tableNameFilter.matcher(table.getTableName()).matches())
    {
      logTableEvent("Skip event, as table name matched filter",
                    event, table);
      return false;
    }

    if (databaseNameFilter != null && databaseNameFilter.matcher(table.getDbName()).matches())
    {
      logTableEvent("Skip event, as database name matched filter",
                    event, table);
      return false;
    }

    log.debug(table.toString());

    if(!table.getParameters().keySet().contains(IcebergTableUtil.metadataLocation))
    {
      logTableEvent("Skip event, as there is no metadata_location in the table TBLPROPERTIES",
              event, table);
      return false;
    }else{
      log.info("metadata_location:"+table.getParameters().get(IcebergTableUtil.metadataLocation));
    }

    return true;
  }
}


