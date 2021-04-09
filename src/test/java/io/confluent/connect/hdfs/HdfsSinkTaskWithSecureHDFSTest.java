/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.avro.AvroDataFileReader;

import static org.junit.Assert.assertEquals;

public class HdfsSinkTaskWithSecureHDFSTest extends TestWithSecureMiniDFSCluster {

  private static final String extension = ".avro";
  private static final String ZERO_PAD_FMT = "%010d";
  private static final int TASKS_NUM = 25;
  private static final long RECORDS_NUM = 100;
  private final DataFileReader schemaFileReader = new AvroDataFileReader();

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(HdfsSinkConnectorConfig.KERBEROS_TICKET_RENEW_PERIOD_MS_CONFIG, "1");
    return props;
  }

  @Test
  public void testSinkTaskPut() throws Exception {
    setUp();
    Map fileSystemCache = getFileSystemCache();
    int fsSizeBefore = fileSystemCache.size();
    HdfsSinkTask task = new HdfsSinkTask();

    String key = "key";
    Schema schema = createSchema();
    Struct record = createRecord(schema);
    Collection<SinkRecord> sinkRecords = new ArrayList<>();
    for (TopicPartition tp : context.assignment()) {
      for (long offset = 0; offset < 7; offset++) {
        SinkRecord sinkRecord =
            new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record,
                           offset);
        sinkRecords.add(sinkRecord);
      }
    }

    task.initialize(context);
    task.start(properties);
    task.put(sinkRecords);
    task.stop();
    task.close(context.assignment());
    // make sure there are no cache entries left
    assertEquals(fsSizeBefore, fileSystemCache.size());

    AvroData avroData = task.getAvroData();
    // Last file (offset 6) doesn't satisfy size requirement and gets discarded on close
    long[] validOffsets = {-1, 2, 5};

    for (TopicPartition tp : context.assignment()) {
      String directory = tp.topic() + "/" + "partition=" + String.valueOf(tp.partition());
      for (int j = 1; j < validOffsets.length; ++j) {
        long startOffset = validOffsets[j - 1] + 1;
        long endOffset = validOffsets[j];
        Path path = new Path(FileUtils.committedFileName(url, topicsDir, directory, tp,
                                                         startOffset, endOffset, extension,
                                                         ZERO_PAD_FMT));
        Collection<Object> records = schemaFileReader.readData(connectorConfig.getHadoopConfiguration(), path);
        long size = endOffset - startOffset + 1;
        assertEquals(records.size(), size);
        for (Object avroRecord : records) {
          assertEquals(avroRecord, avroData.fromConnectData(schema, record));
        }
      }
    }
  }

  @Test
  public void testSinkTaskPutMultipleTasks() throws Exception {
    setUp();
    Map fileSystemCache = getFileSystemCache();
    int fsSizeBefore = fileSystemCache.size();
    for (int i = 0; i < TASKS_NUM; i++) {
      HdfsSinkTask task = new HdfsSinkTask();

      String key = "key";
      Schema schema = createSchema();
      Struct record = createRecord(schema);
      Collection<SinkRecord> sinkRecords = new ArrayList<>();
      for (TopicPartition tp : context.assignment()) {
        for (long offset = 0; offset < RECORDS_NUM / context.assignment().size(); offset++) {
          SinkRecord sinkRecord =
              new SinkRecord(tp.topic(), tp.partition(), Schema.STRING_SCHEMA, key, schema, record,
                  offset);
          sinkRecords.add(sinkRecord);
        }
      }

      task.initialize(context);
      task.start(properties);
      task.put(sinkRecords);
      task.stop();
      task.close(context.assignment());
      // make sure there are no cache entries left
      assertEquals(fsSizeBefore, fileSystemCache.size());
    }
    // make sure there are no cache entries left
    assertEquals(fsSizeBefore, fileSystemCache.size());
  }
}
