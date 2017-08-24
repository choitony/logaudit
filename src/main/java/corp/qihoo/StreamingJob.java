package corp.qihoo;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import corp.qihoo.bigaudit.es.ESSink;
import corp.qihoo.bigaudit.kafka.KafkaSourceZPE;
import corp.qihoo.bigaudit.log.HdfsNameLog;
import corp.qihoo.bigaudit.operator.Filter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileReader;
import java.util.*;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/log-audit-001-0.0.0.jar
 * From the CLI you can then run
 * 		./bin/flink run -c corp.qihoo.StreamingJob target/log-audit-001-0.0.0.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/**
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		String propertiesFile = "/home/xitong/software/flink/bigaudit/bigaudit";
		ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);

		for(Map.Entry key : (parameter.toMap()).entrySet()){
			System.out.println("" + key.getKey() + ": "+key.getValue());
		}

		KafkaSourceZPE kafkaSourceZPE = new KafkaSourceZPE(args[0]);

		DataStream<String> namenodeLog = env.addSource(kafkaSourceZPE.getKafkaSource());

		Filter filter = new Filter();
		HdfsNameLog hdfsNameLog = new HdfsNameLog(parameter);
		namenodeLog.flatMap(hdfsNameLog.getFlatMapFunction())
				.filter(hdfsNameLog.getFilterFunction())
				.map(hdfsNameLog.getMapFunction())
				.addSink(ESSink.getESSink());

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
