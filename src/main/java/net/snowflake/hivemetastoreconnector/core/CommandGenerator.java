/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.core;

import net.snowflake.hivemetastoreconnector.SnowflakeIcebergListener;
import net.snowflake.hivemetastoreconnector.commands.Command;
import net.snowflake.hivemetastoreconnector.commands.CreateIcebergTable;
import net.snowflake.hivemetastoreconnector.commands.DropIcebergTable;
import net.snowflake.hivemetastoreconnector.commands.AlterIcebergTable;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that generates the commands to be executed
 */
public class CommandGenerator
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeIcebergListener.class);

  private static final Logger iceberglog =
          LoggerFactory.getLogger(SnowflakeIcebergListener.class);

  /**
   * Creates a command based on the arguments
   * Defers the actual creation to subclasses
   * @param event - the event passed from the hive metastore
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   * @return a command corresponding to the command to be executed
   */
  public static Command getIcebergCommand(ListenerEvent event,
                                   SnowflakeConf snowflakeConf)
  {
    iceberglog.info(String.format("Get command executed (%s)",
            event.getClass().getSimpleName()));
    Command command = null;
    if (event instanceof CreateTableEvent)
    {
      iceberglog.info("Generating Create Table command");
      command = new CreateIcebergTable((CreateTableEvent)event, snowflakeConf);
    }
    else if (event instanceof DropTableEvent)
    {
      iceberglog.info("Generating Drop Table command");
      command = new DropIcebergTable((DropTableEvent)event, snowflakeConf);
    }
    else if (event instanceof AlterTableEvent)
    {
      iceberglog.info("Generating Alter Table command");
      command = new AlterIcebergTable((AlterTableEvent)event, snowflakeConf);
    }
    try {
      iceberglog.info("command:" + command.generateSqlQueries());
    }catch (Exception e){
      e.printStackTrace();
    }
    return command;
  }
}