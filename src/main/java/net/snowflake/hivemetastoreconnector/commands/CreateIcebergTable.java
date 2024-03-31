/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.commands;

import com.google.common.base.Preconditions;
import net.snowflake.hivemetastoreconnector.util.IcebergTableUtil;
import net.snowflake.hivemetastoreconnector.SnowflakeConf;
import net.snowflake.hivemetastoreconnector.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * A class for the CreateExternalTable command
 *
 * @author xma
 */
public class CreateIcebergTable extends Command {
    private static final Logger log =
            LoggerFactory.getLogger(CreateIcebergTable.class);

    /**
     * Creates a CreateExternalTable command
     *
     * @param createTableEvent Event to generate a command from
     * @param snowflakeConf    - the configuration for Snowflake Hive metastore
     *                         listener
     */
    public CreateIcebergTable(CreateTableEvent createTableEvent,
                              SnowflakeConf snowflakeConf) {
        this(Preconditions.checkNotNull(createTableEvent).getTable(),
                snowflakeConf,
                createTableEvent.getHandler().getConf(),
                true);
    }

    /**
     * Creates a CreateExternalTable command, without an event
     *
     * @param hiveTable     The Hive table to generate a command from
     * @param snowflakeConf The configuration for Snowflake Hive metastore
     *                      listener
     * @param hiveConf      The Hive configuration
     * @param canReplace    Whether to replace existing resources or not
     */
    public CreateIcebergTable(Table hiveTable,
                              SnowflakeConf snowflakeConf,
                              Configuration hiveConf,
                              boolean canReplace) {
        super(hiveTable);
        this.hiveTable = Preconditions.checkNotNull(hiveTable);
        this.snowflakeConf = Preconditions.checkNotNull(snowflakeConf);
        this.hiveConf = Preconditions.checkNotNull(hiveConf);
        this.canReplace = canReplace;
    }

    /**
     * Helper method to get the version of the connector (aka the Maven
     * artifact version).
     *
     * @return The Hive metastore connector version
     */
    private static String getConnectorVersion() {
        return CreateIcebergTable.class.getPackage().getImplementationVersion();
    }

    /**
     * Generates the queries for create external table
     * The behavior of this method is as follows (in order of preference):
     * a. If the user specifies an integration, use the integration to create
     * a stage. Then, use the stage to create a table.
     * b. If the user specifies a stage, use the stage to create a table.
     * c. If the user allows this listener to read from Hive configs, use the AWS
     * credentials from the Hive config to create a stage. Then, use the
     * stage to create a table.
     * d. Raise an error. Do not create a table.
     *
     * @return The Snowflake query generated
     * @throws SQLException                  Thrown when there was an error executing a Snowflake
     *                                       SQL query (if a Snowflake query must be executed).
     * @throws UnsupportedOperationException Thrown when the input is invalid
     * @throws IllegalArgumentException      Thrown when arguments are illegal
     */
    public List<String> generateSqlQueries()
            throws SQLException, UnsupportedOperationException,
            IllegalArgumentException {
        List<String> queryList = new ArrayList<>();
//
//    "CREATE ICEBERG TABLE myIcebergTable" +
//            "  EXTERNAL_VOLUME='icebergMetadataVolume'" +
//            "  CATALOG='icebergCatalogInt'" +
//            "  BASE_LOCATION='airlines/airlines'"+
//            "  METADATA_FILE_PATH='path/to/metadata/v1.metadata.json';"
        queryList.add(String.format("CREATE OR REPLACE ICEBERG TABLE %s" +
                        " EXTERNAL_VOLUME='%s'" +
                        " CATALOG='%s'" +
                        " BASE_LOCATION='%s'" +
                        " METADATA_FILE_PATH='%s';",
                StringUtil.escapeSqlIdentifier(hiveTable.getTableName()),
                hiveTable.getParameters().get(IcebergTableUtil.sfExternalVolume),
                hiveTable.getParameters().get(IcebergTableUtil.sfCatalog),
                hiveTable.getParameters().get(IcebergTableUtil.sfBaseLocation),
                IcebergTableUtil.getMetadataLocation(hiveTable)));

//       if(!table.getParameters().keySet().contains(sfCatalog) || !table.getParameters().keySet().contains(sfExternalVolume) || !table.getParameters().keySet().contains(sfBaseLocation))
        // Add the connector version in the comments
//        sb.append(" COMMENT='Generated with Hive metastore connector (version=");
//        sb.append(getConnectorVersion());
//        sb.append(").'");
//
//        sb.append(";");
        return queryList;
    }

    private final Table hiveTable;

    private final Configuration hiveConf;

    private final SnowflakeConf snowflakeConf;

    private boolean canReplace;

}
