package org.weikey.hbase.bulkload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class HFileMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	private String tableName;
	private String tablestruct[];
	private String qualifiers[];
	private Integer qualifiersPos[];
	private String rowKeyQualifiers[];
	private Integer keysPos[];
	private Function_substring subConfig;
	private String[] columnValues;

	private TableIndexs tableIndexs;
	private String fieldDelimiter;

	private MapInterface mapInterface;
	private boolean appendUuid = false;

	private HTable htable;
	private boolean putFlag = false;

	private final static String keyDelimiter = Constants.FIELD_DELIMITER;
	private final static byte family[] = Bytes.toBytes("s");

	@Override
	public void map(LongWritable offset, Text value, Context context)
			throws IOException {
		mapInterface.map(offset, value, context);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		tableName = context.getConfiguration().get("tableName");
		tablestruct = getQualifier(context.getConfiguration()
				.get("tablestruct"));
		qualifiers = getQualifier(context.getConfiguration().get("qualifiers"));
		rowKeyQualifiers = getQualifier(context.getConfiguration().get(
				"rowKeyQualifiers"));
		qualifiersPos = getQualifierPos(tablestruct, qualifiers);
		keysPos = getQualifierPos(tablestruct, rowKeyQualifiers);
		// Add a little configuration to handle rowkey composer which require
		// substring(column,startIndex,endIndex) function
		subConfig = processRowKeyConfig(tablestruct, rowKeyQualifiers);

		fieldDelimiter = /* String.valueOf((char) 1); */context
				.getConfiguration().get("fieldDelimiter");
		appendUuid = context.getConfiguration().getBoolean("appenduuid", true);
		putFlag = context.getConfiguration().getBoolean("put", false);

		this.tableIndexs = new TableIndexs(tableName, tablestruct, context
				.getConfiguration().get("loadindexs"),
				context.getConfiguration());
		if (tableIndexs.getIndexNum() == 0) { // 没有导入索引表
			mapInterface = new NormalTableMap();
		} else { // 需要导入索引表
			mapInterface = new TableWithIndexsMap();
		}

		if (putFlag) {
			htable = new HTable(context.getConfiguration(), tableName);
			htable.setAutoFlush(false);
			htable.setWriteBufferSize(20 * 1024 * 1024);
		}

		super.setup(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException {
		if (htable != null) {
			htable.flushCommits();
			htable.close();
		}
		tableIndexs.cleanup(context);
	}

	private Put createRowPut(byte rowkey[], String columnValues[]) {
		Put put = new Put(rowkey);
		long ts = System.currentTimeMillis();
		try {
			for (int i = 0; i < qualifiersPos.length; i++) {
				KeyValue kv = new KeyValue(rowkey, family,
						Bytes.toBytes(qualifiers[i]), ts, KeyValue.Type.Put,
						Bytes.toBytes(columnValues[qualifiersPos[i]]));
				put.add(kv);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return put;
	}

	private byte[] createRowkey(String columnValues[], Integer keysPos[]) {
		byte[] rowkey = null;
		if (columnValues != null && columnValues.length > 0) {
			StringBuffer rowkeyBuffer = new StringBuffer();
			for (int i = 0; i < keysPos.length; i++) {
				if (i != 0) {
					rowkeyBuffer.append(keyDelimiter);
				}
				if (subConfig.match(keysPos[i])) {
					rowkeyBuffer.append(columnValues[keysPos[i]].substring(
							subConfig.getStartIndex(keysPos[i]),
							subConfig.getEndIndex(keysPos[i])));
				} else {
					rowkeyBuffer.append(columnValues[keysPos[i]]);
				}
			}
			if (appendUuid) {
				rowkeyBuffer.append(Constants.ROWKEY_END);
				rowkeyBuffer.append(Util.getRowkeyId());
			}
			rowkey = Bytes.toBytes(rowkeyBuffer.toString());
		}
		return rowkey;
	}

	private String[] getColumnValues(String line) {
		String columnValues[] = null;

		if (line != null && !line.isEmpty()) {
			columnValues = line.split(this.fieldDelimiter, tablestruct.length);
			if (columnValues.length != tablestruct.length) {
				System.out.println("Incorrect record with "
						+ columnValues.length + " columns while "
						+ tablestruct.length + " columns expected");
				System.out.println(line);
			}
		}
		// String ret[][] = {fields, columnValues};
		return columnValues;
	}

	private String[] getQualifier(String text) {
		String qualifier[] = text.split(",");
		return qualifier;
	}

	private Function_substring processRowKeyConfig(String[] tablestruct,
			String[] rowkeyConstitues) {
		Function_substring substring_config = new Function_substring();
		for (int i = 0; i < rowkeyConstitues.length; i++) {
			String rowKeyPart = rowkeyConstitues[i];
			if (rowKeyPart.contains(Constants.FUNC_SUBSTRING)) {
				int left = rowKeyPart.indexOf("(") + 1;
				int right = rowKeyPart.indexOf(".");
				int lastDotIndex = rowKeyPart.lastIndexOf(".");
				String column = rowKeyPart.substring(left, right);
				String startIndex = rowKeyPart.substring(right + 1,
						lastDotIndex);
				String endIndex = rowKeyPart.substring(lastDotIndex + 1,
						rowKeyPart.lastIndexOf(")"));
				int keypos = getPos(tablestruct, column);
				substring_config.addPos(keypos, Integer.valueOf(startIndex),
						Integer.valueOf(endIndex));
			}
		}
		return substring_config;
	}

	private class Function_substring {
		List<Integer> processKeyPos = null;
		HashMap<Integer, Integer> startIndex = null;
		HashMap<Integer, Integer> endIndex = null;

		public Function_substring() {
			processKeyPos = new ArrayList<Integer>();
			startIndex = new HashMap<Integer, Integer>();
			endIndex = new HashMap<Integer, Integer>();
		}

		public void addPos(int pos, int start, int end) {
			processKeyPos.add(pos);
			startIndex.put(pos, start);
			endIndex.put(pos, end);
		}

		public boolean match(int pos) {
			return processKeyPos.contains(pos);
		}

		public int getStartIndex(int keyPos) {
			return startIndex.get(keyPos);
		}

		public int getEndIndex(int keyPos) {
			return endIndex.get(keyPos);
		}
	}

	private interface MapInterface {
		void map(LongWritable offset, Text value, Context context)
				throws IOException;
	}

	private class NormalTableMap implements MapInterface {
		public void map(LongWritable offset, Text value, Context context)
				throws IOException {
			try {
				String line = value.toString();
				columnValues = getColumnValues(line); // field[0]选中字段，field[1]全部字段
				byte rowkey[] = createRowkey(columnValues, keysPos);
				if (rowkey != null && rowkey.length > 0) {
					if (putFlag) {
						Put put = createRowPut(rowkey, columnValues);
						put.setWriteToWAL(false);
						htable.put(put);
					} else {
						ImmutableBytesWritable ky = new ImmutableBytesWritable(
								rowkey);
						context.write(ky, createRowPut(rowkey, columnValues));
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private class TableWithIndexsMap implements MapInterface {
		private Put createIndexRowPut(byte rowkey[], byte tableRowkey[]) {
			Put put = new Put(rowkey);
			try {
				KeyValue kv = new KeyValue(rowkey, family, Bytes.toBytes("v"),
						1, KeyValue.Type.Put, tableRowkey);
				put.add(kv);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return put;
		}

		private byte[] tableMap(Context context) throws IOException {
			try {
				byte rowkey[] = createRowkey(columnValues, keysPos);
				if (rowkey != null && rowkey.length > 0) {
					if (putFlag) {
						Put put = createRowPut(rowkey, columnValues);
						put.setWriteToWAL(false);
						htable.put(put);
					} else {
						String rowkeyBuffer = tableName
								+ Constants.TABLE_ROWKEY_DELIMITER
								+ new String(rowkey);
						ImmutableBytesWritable ky = new ImmutableBytesWritable(
								Bytes.toBytes(rowkeyBuffer));
						context.write(ky, createRowPut(rowkey, columnValues));
					}
					return rowkey;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return new byte[0];
		}

		private void indexMap(Context context, byte[] tableRowkey)
				throws IOException, InterruptedException {
			int indexNum = tableIndexs.getIndexNum();

			for (int i = 0; i < indexNum; i++) {
				if (Bytes.toString(tableIndexs.getIndexTable(i).getTableName()).equals(
						"sysindex_loganalysis_3_Vontu_sender_time_row_id")
						&& !columnValues[1].equals("Block")) {
					continue;
				}
				byte rowkey[] = createRowkey(columnValues,
						tableIndexs.getIndexPos(i));
				if (rowkey != null && rowkey.length > 0) {
					tableIndexs.getIndexTable(i).put(
							createIndexRowPut(rowkey, tableRowkey));
				}
			}
		}

		public void map(LongWritable offset, Text value, Context context)
				throws IOException {
			String line = value.toString();
			columnValues = getColumnValues(line);
			byte tableRowkey[] = tableMap(context);
			try {
				indexMap(context, tableRowkey);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private class TableIndexs {
		private String indexNames[]; // 索引表名
		private Integer indexsPos[][]; // 索引字段在表中的位置
		private HTable indexTables[];

		public TableIndexs(String tableName, String tablestruct[],
				String indexs, Configuration conf) throws IOException {
			if (indexs == null || indexs.length() == 0) {
				indexNames = new String[0];
				indexsPos = new Integer[0][];
				indexTables = new HTable[0];
			} else {
				String indexArray[] = indexs.split(";");
				this.indexNames = new String[indexArray.length];
				this.indexsPos = new Integer[indexArray.length][];
				this.indexTables = new HTable[indexArray.length];
				for (int i = 0; i < indexArray.length; i++) {
					String index[] = indexArray[i].trim().split(",");
					this.indexsPos[i] = HFileMapper
							.getQualifierPos(tablestruct, index);
					this.indexNames[i] = "sysindex_" + tableName;
					for (int n = 0; n < index.length; n++) {
						this.indexNames[i] += "_";
						int begin = index[n].indexOf("(");
						if (begin == -1) {
							indexNames[i] += index[n];
						} else {
							int end = index[n].indexOf(".", begin);
							indexNames[i] += index[n].substring(begin + 1, end);
						}

					}

					this.indexTables[i] = new HTable(conf, this.indexNames[i]);
					this.indexTables[i].setAutoFlush(false);
					this.indexTables[i].setWriteBufferSize(4 * 1024 * 1024);
				}
			}
		}

		/**
		 * 获取索引数量
		 * 
		 * @return
		 */
		public int getIndexNum() {
			return indexNames.length;
		}

		/**
		 * 获取指定索引字段在原表中的位置
		 * 
		 * @param index
		 * @return
		 */
		public Integer[] getIndexPos(int index) {
			return indexsPos[index];
		}

		/*
		 * public String getIndexName(int index){ return indexNames[index]; }
		 */

		public HTable getIndexTable(int index) {
			return this.indexTables[index];
		}

		public void cleanup(Context context) throws IOException {
			for (HTable htable : indexTables) {
				htable.flushCommits();
				htable.close();
			}
		}

	}

	/**
	 * 取qualifier在表结构中的位置
	 * 
	 * @param tablestruct
	 * @param qualifier
	 * @return qualifier对应的位置
	 */
	private static Integer[] getQualifierPos(String tablestruct[],
			String qualifier[]) {
		Integer qualifierPos[] = new Integer[qualifier.length];
		for (int i = 0; i < qualifier.length; i++) {
			qualifierPos[i] = getPos(tablestruct, qualifier[i]);
		}
		return qualifierPos;
	}

	/**
	 * 取单个列在表结构中的位置
	 * 
	 * @param tablestruct
	 * @param col
	 * @return col位置
	 */
	private static int getPos(String tablestruct[], String col) {
		for (int i = 0; i < tablestruct.length; i++) {
			if (tablestruct[i].equals(col)) {
				return i;
			} else {
				if (col.contains(Constants.FUNC_SUBSTRING)) {
					int left = col.indexOf("(") + 1;
					int right = col.indexOf(".");
					String column = col.substring(left, right);
					if (tablestruct[i].equals(column)) {
						return i;
					}
				}
			}
		}
		return -1;
	}
}
