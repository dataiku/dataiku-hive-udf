package com.dataiku.hive.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

import java.util.Map;
import java.util.Properties;

/**
 */
public class XMLHiveStorageHandler extends DefaultStorageHandler {
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
    return XMLHiveInputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return XMLSerde.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat.class;
    }



    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
           super.configureInputJobProperties(tableDesc, jobProperties);    //To change body of overridden methods use File | Settings | File Templates.
        Properties props = tableDesc.getProperties();
        jobProperties.put(XMLHiveInputFormat.TAG_KEY, props.getProperty(XMLHiveInputFormat.TAG_KEY));
    }
}
