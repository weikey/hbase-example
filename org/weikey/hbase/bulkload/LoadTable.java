package org.weikey.hbase.bulkload;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.ArrayDeque;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LoadTable {
	public static class HBaseReducer extends
			Reducer<NullWritable, Text, NullWritable, Text> {
		/**
		 * 合并作用
		 */
		@Override
		protected void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(key, t);
			}
		}
	}

	private final static String LOCK = "s";

	@SuppressWarnings("deprecation")
	public boolean load(String source, String table, String sourcestruct,
			String qualifiers, String rowKeyQualifiers, String fieldDelimiter,
			String splits[], boolean appendUuid, boolean put,
			List<String> indexs) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		// 可在配置文件中设置，或可根据实际运行环境设置
		/*
		 * conf.set("hbase.cluster.distributed", "true");
		 * conf.set("hbase.rootdir", "hdfs://masternode/hbase");
		 * conf.set("hbase.zookeeper.quorum", "slavenode6");
		 */

		SchemaMetrics.configureGlobally(conf);
		// conf.set("textinputformat.record.delimiter", "\n");

		StringBuffer loadIndexs = new StringBuffer();
		for (Iterator<String> iter = indexs.iterator(); iter.hasNext();) {
			String index = iter.next();
			loadIndexs.append(index);
			loadIndexs.append(";");
		}

		conf.set("tableName", table);
		conf.set("tablestruct", sourcestruct);
		conf.set("qualifiers", qualifiers);
		conf.set("rowKeyQualifiers", rowKeyQualifiers);
		conf.set("fieldDelimiter", fieldDelimiter);
		conf.set("loadindexs", loadIndexs.toString());
		conf.setBoolean("appenduuid", appendUuid);
		conf.setBoolean("put", put);

		String output = new String("/tmp/tmp_" + table + "_"
				+ Util.getRowkeyId() + "_tmp");

		HBaseAdmin hbadmin = null;
		// 创建索引表
		hbadmin = new HBaseAdmin(conf);
		StringBuffer indexNames[] = new StringBuffer[indexs.size()]; // 索引表名，命名格式为：sysindex_tablename_indexcol1_indexcol2...
		HTable htableIndexs[] = new HTable[indexNames.length];
		if (indexs.size() > 0) {
			int i = 0;
			for (Iterator<String> iter = indexs.iterator(); iter.hasNext(); i++) {
				indexNames[i] = new StringBuffer();
				indexNames[i].append("sysindex_" + table);
				String index = iter.next();
				String indexArray[] = index.split(",");
				for (int n = 0; n < indexArray.length; n++) {
					indexNames[i].append("_");
					int begin = indexArray[n].indexOf("(");
					if (begin == -1) {
						indexNames[i].append(indexArray[n]);
					} else {
						int end = indexArray[n].indexOf(".", begin);
						indexNames[i].append(indexArray[n].substring(begin + 1,
								end));
					}
				}
				HTableDescriptor htd = new HTableDescriptor(
						indexNames[i].toString());
				HColumnDescriptor hcd = new HColumnDescriptor("s");
				htd.addFamily(hcd);
				if (!hbadmin.tableExists(indexNames[i].toString())) {
					hbadmin.createTable(htd);
				}
				htableIndexs[i] = new HTable(conf, htd.getName());
			}
		}
		// create table
		HTableDescriptor htd = new HTableDescriptor(table);
		HColumnDescriptor hcd = new HColumnDescriptor("s");
		htd.addFamily(hcd);
		// 分区可根据实际环境优化
		if (splits != null && splits.length > 1) {
			byte regions[][] = Util.getSplits(splits);
			if (hbadmin.tableExists(table) == false) {
				htd.setMaxFileSize(100l * 1024l * 1024l * 1024l);
				hcd.setCompressionType(Algorithm.SNAPPY);
				// htd
				hbadmin.createTable(htd, regions);
			}
		} else {
			if (hbadmin.tableExists(table) == false) {
				hbadmin.createTable(htd);
			}
		}
		HTable htable = new HTable(conf, table);

		try {
			Job job = new Job(conf, "BulkLoad " + table);

			if (put) {
				job.setJarByClass(LoadTable.class);
				job.setMapperClass(HFileMapper.class);
				job.setReducerClass(HBaseReducer.class);

				job.setOutputKeyClass(NullWritable.class);
				job.setOutputValueClass(Text.class);
				job.setNumReduceTasks(0);// 不做reduce
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				FileInputFormat.addInputPath(job, new Path(source));
				FileOutputFormat.setOutputPath(job, new Path(output));
				System.out.println("using mapreduce put");
			} else {
				job.setJarByClass(LoadTable.class);
				job.setMapperClass(HFileMapper.class);
				job.setReducerClass(PutSortReducer.class);

				job.setMapOutputKeyClass(ImmutableBytesWritable.class);
				job.setMapOutputValueClass(Put.class);

				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(HFileOutputFormat.class);

				FileInputFormat.addInputPath(job, new Path(source));
				FileOutputFormat.setOutputPath(job, new Path(output));

				/*
				 * if (indexs.size() != 0){ //有索引表
				 * job.setOutputFormatClass(MultiHFileOutputFormat.class);
				 * job.setPartitionerClass(MultiTotalOrderPartitioner.class); }
				 */
				HFileOutputFormat.configureIncrementalLoad(job, htable);
			}

			job.waitForCompletion(true);

			// Do bulkload
			try {
				Runtime rt = Runtime.getRuntime();
				rt.exec("hadoop fs -chmod 777 " + output + "/*");
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (put == false) {
				LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
				load.doBulkLoad(new Path(output), htable);
			}
		} finally {
			LoadTable.deletePath(conf, output);
			if (hbadmin != null) {
				hbadmin.close();
			}
		}

		return true;
	}

	/**
	 * 根据配置文件设置加载数据
	 * 
	 * @param configFileName
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean load(String configFileName) throws IOException,
			InterruptedException {
		BulkloadConfig bulkloadConfig = new BulkloadConfig();
		bulkloadConfig.parseConfigFile(configFileName);

		List<BulkloadConfig.LoadInformation> loadInformations = bulkloadConfig
				.getLoadInformations();
		int threadNum = bulkloadConfig.getThreadNum();

		final Queue<BulkloadConfig.LoadInformation> loadInformationsQueue = new ArrayDeque<BulkloadConfig.LoadInformation>();
		for (Iterator<BulkloadConfig.LoadInformation> iter = loadInformations
				.iterator(); iter.hasNext();) {
			loadInformationsQueue.add(iter.next());
		}

		// 多线程处理
		Thread threads[] = new Thread[threadNum];
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new Thread() {
				@Override
				public void run() {
					for (;;) {
						BulkloadConfig.LoadInformation info = null;
						synchronized (LOCK) {
							info = loadInformationsQueue.poll();
						}
						if (info == null)
							break;
						if (info.fieldDelimiter.startsWith("\\")) {
							info.fieldDelimiter = String
									.valueOf((char) Integer
											.parseInt(info.fieldDelimiter
													.substring(1)));
						} else if (info.fieldDelimiter.equals("|")) {
							info.fieldDelimiter = "\\|";
						} else if (info.fieldDelimiter.equals("|@|")) {
							info.fieldDelimiter = "\\|@\\|";
						}
						System.out.println(info.fieldDelimiter);
						try {
							LoadTable loadTable = new LoadTable();
							System.out.println("put=" + info.put);
							loadTable.load(info.inputPath, info.tableName,
									info.tableStruct, info.loadCols,
									info.rowkeyCols, info.fieldDelimiter,
									info.spilts, info.appendUuid, info.put,
									info.indexs);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			};
			threads[i].start();
		}
		for (int i = 0; i < threads.length; i++) {
			threads[i].join();
		}

		return true;
	}

	@SuppressWarnings("deprecation")
	public static void deletePath(Configuration conf, String path)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(path));
		fs.close();
	}

	public static void main(String[] args) throws Exception {
		LoadTable.load(args[0]);
	}
}
