/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.hivemetastoreconnector.util;


import org.apache.hadoop.hive.metastore.api.Table;

public class IcebergTableUtil
{
  public static String metadataLocation = "metadata_location";

  public static String sfCatalog = "CATALOG";

  public static String sfExternalVolume = "EXTERNAL_VOLUME";

  public static String sfBaseLocation = "BASE_LOCATION";

//  public static String isIcebergTable = "isIcebergTable";

  public static Boolean isAbletoCreateTable(Table table){
    if(table.getParameters().keySet().contains(IcebergTableUtil.sfCatalog) &&
            table.getParameters().keySet().contains(IcebergTableUtil.sfExternalVolume) &&
            table.getParameters().keySet().contains(IcebergTableUtil.sfBaseLocation)){
      return true;
    }
    return false;
  }

  public static String getMetadataLocation(Table table){
    String fullPath=table.getParameters().get(metadataLocation);
    int metadataIndex = fullPath.indexOf("/metadata/");
    return fullPath.substring(metadataIndex + 1);
  }

}
