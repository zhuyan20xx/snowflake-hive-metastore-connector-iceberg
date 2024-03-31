/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.commands;

import com.google.common.base.Preconditions;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.util.IcebergTableUtil;
import net.snowflake.hivemetastoreconnector.util.StringUtil;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;

import java.util.ArrayList;
import java.util.List;

/**
 * A class for the DropExternalTable command
 */
public class DropIcebergTable extends Command
{
  /**
   * Creates a DropExternalTable command
   * @param dropTableEvent Event to generate a command from
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public DropIcebergTable(DropTableEvent dropTableEvent,
                          SnowflakeConf snowflakeConf)
  {
    super(Preconditions.checkNotNull(dropTableEvent).getTable());
    this.hiveTable = Preconditions.checkNotNull(dropTableEvent.getTable());
    this.snowflakeConf = Preconditions.checkNotNull(snowflakeConf);
  }

  /**
   * Generates the necessary queries on a hive drop table event
   * @return The Snowflake queries generated
   */
  public List<String> generateSqlQueries()
  {
    List<String> queryList = new ArrayList<>();
    queryList.add(String.format("DROP ICEBERG IF EXISTS TABLE %s;",
            StringUtil.escapeSqlIdentifier(hiveTable.getTableName())));

    return queryList;
  }

  private final Table hiveTable;

  private final SnowflakeConf snowflakeConf;
}
