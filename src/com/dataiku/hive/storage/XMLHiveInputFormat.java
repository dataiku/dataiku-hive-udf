/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * Adapted from XmlInputFormat from Mahout project
 */
package com.dataiku.hive.storage;

import java.io.IOException;
import java.util.Properties;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import sun.nio.cs.ext.MacHebrew;

/**
 * Reads records that are delimited by a specfic begin/end tag.
 */
public class XMLHiveInputFormat extends TextInputFormat {

    public static final String TAG_KEY = "xml.tag";

    public static final Log LOG = LogFactory.getLog(XMLHiveInputFormat.class.getName());


    @Override
    public RecordReader<LongWritable,Text> getRecordReader(InputSplit inputSplit,
                                                           JobConf jobConf,
                                                           Reporter reporter) throws IOException {
        return new XmlRecordReader((FileSplit) inputSplit, jobConf);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }

    /**
     * XMLRecordReader class to read through a given xml document to output xml
     * blocks as records as specified by the start tag and end tag
     *
     */
    public static class XmlRecordReader implements
            RecordReader<LongWritable,Text> {
        private final byte[] startTag;
        private final byte[] endTag;
        private final long start;
        private final long end;
        private final FSDataInputStream fsin;
        private final DataOutputBuffer buffer = new DataOutputBuffer();

        public XmlRecordReader(FileSplit split, JobConf jobConf) throws IOException {
            String tagKey = jobConf.get(TAG_KEY);
            if (tagKey == null) {
                try {
                    Properties tableProperties = Utilities.getMapRedWork(jobConf).getPathToPartitionInfo().get(getInputPaths(jobConf)[0].toString()).getTableDesc().getProperties() ;
                    tagKey = tableProperties.getProperty(TAG_KEY);
                }   catch (Exception e) {
                    throw new IOException("Unable to retrieve value for " + TAG_KEY, e);
                }
                if (tagKey == null) {
                    throw new IOException("Unable to retrieve value for " + TAG_KEY);
                }
            }
            String startTagString = "<" + tagKey;
            String endTagString = "</" + tagKey + ">";
            startTag = startTagString.getBytes("utf-8");
            endTag = endTagString.getBytes("utf-8");

            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(jobConf);
            fsin = fs.open(split.getPath());
            fsin.seek(start);
            LOG.info("Initialized XmlRecordReader  with tag " + tagKey + " to " + split.getPath().toString());
        }


        protected boolean readUntilSlashOrOpenTag(LongWritable key, Text value, int b) throws IOException {
            while (true) {
                if (b == -1) {
                    return false;
                }
                writeToBuffer(b);
                if (b == (int) '/') {
                    b = fsin.read();
                    writeToBuffer(b);
                    if (b == (int) '>') {
                        key.set(fsin.getPos());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                }

                if (b == (int) '>') {
                    return false;
                }
                b = fsin.read();
           }
        }

        @Override
        public boolean next(LongWritable key, Text value) throws IOException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {

                        int b = fsin.read();
                        if (Character.isLetterOrDigit(b)) {
                            return next(key, value);  // <TAGS> should not  match <TAG !!
                        }
                        buffer.write(startTag);


                        // Are we in <TAG />  or <TAG> ... </TAG> ?

                        if (readUntilSlashOrOpenTag(key, value, b)) {
                            //LOG.info("Key:" + key.toString() + " Value:" + value.toString());
                            return true;
                        }

                        // Read until match tag...
                        if (readUntilMatch(endTag, true)) {
                            key.set(fsin.getPos());
                            value.set(buffer.getData(), 0, buffer.getLength());

                            //String s = value.toString();
                            //LOG.info("Key:" + key.toString() + " Begin:" + s.substring(0, Math.min(s.length(), 10))
                            //+ " End:" + s.substring(s.length() - Math.min(s.length(), 10), s.length()) + "#");
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return fsin.getPos();
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        @Override
        public float getProgress() throws IOException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        private void writeToBuffer(int b) throws IOException {
            if (b == '\n') {
                buffer.write(' ');
            } else if (b != '\r') {
                buffer.write(b);
            }
        }

        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) return false;
                // save to buffer:
                if (withinBlock) {
                    writeToBuffer(b);
                }

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) return true;
                } else i = 0;
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
            }
        }
    }
}