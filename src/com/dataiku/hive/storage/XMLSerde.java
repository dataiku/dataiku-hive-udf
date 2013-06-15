package com.dataiku.hive.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * Serde for
 */
public class XMLSerde implements SerDe {


    ObjectInspector oi;
    List<String> row;
    public static final Log LOG = LogFactory.getLog(XMLSerde.class.getName());

    @Override
    public void initialize(Configuration entries, Properties properties) throws SerDeException {
        List<String> columnNames = new ArrayList<String>();
        columnNames.add("text");

        ArrayList<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());

        columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        oi = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
        row = new ArrayList<String>();
        row.add(null);
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        Text rowText = (Text) blob;
         row.set(0, rowText.toString());
        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return oi;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
        throw new SerDeException("Not implemented");

    }
}
