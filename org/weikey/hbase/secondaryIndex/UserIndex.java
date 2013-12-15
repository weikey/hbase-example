package org.weikey.hbase.secondaryIndex;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * usage:
 1. disable 'table'

 2. alter 'table', METHOD=>'table_att','coprocessor'=>'hdfs:///hbase-coprocessor.jar|CCB_POC.LogAnalysis.TC_ISAC_03.UserIndex|1001'

 3. enable 'table'

 4. create index table
 */

public class UserIndex extends BaseRegionObserver {

	private HTablePool pool = null;

	private final static String INDEX_TABLE = "loganalysis_3_user_index";
	private final static String SOURCE_TABLE = "loganalysis_3";

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		pool = new HTablePool(env.getConfiguration(), 10);
	}

	@Override
	public void postPut(
			final ObserverContext<RegionCoprocessorEnvironment> observerContext,
			final Put put, final WALEdit edit, final boolean writeToWAL)
			throws IOException {

		byte[] table = observerContext.getEnvironment().getRegion()
				.getRegionInfo().getTableName();

		// Not necessary though if you register the coprocessor
		// for the specific table, SOURCE_TBL
		if (!Bytes.equals(table, Bytes.toBytes(SOURCE_TABLE))) {
			return;
		}

		try {
			// get the values
			HTableInterface table2 = pool.getTable(Bytes.toBytes(INDEX_TABLE));
			List<KeyValue> kv1 = put.get(Bytes.toBytes("cf1"),
					Bytes.toBytes("sender"));
			List<KeyValue> kv2 = put.get(Bytes.toBytes("cf1"),
					Bytes.toBytes("type"));
			List<KeyValue> kv3 = put.get(Bytes.toBytes("cf1"),
					Bytes.toBytes("row_id"));
			if (kv1 == null || kv2 == null || kv3 == null)
				return;
			Iterator<KeyValue> kvItor1 = kv1.iterator();
			Iterator<KeyValue> kvItor2 = kv2.iterator();
			Iterator<KeyValue> kvItor3 = kv3.iterator();
			long timestamp = 0l;
			while (kvItor1.hasNext()) {
				KeyValue tmp1 = kvItor1.next();
				KeyValue tmp2 = kvItor2.next();
				KeyValue tmp3 = kvItor3.next();
				Put indexPut = new Put(Bytes.toBytes(Bytes.toString(tmp1
						.getValue())
						+ Bytes.toString(tmp2.getValue())
						+ Bytes.toString(tmp3.getValue())));
				indexPut.add(Bytes.toBytes("cf1"), Bytes.toBytes("time_rowid"),
						tmp1.getRow());
				table2.put(indexPut);
			}
			table2.close();

		} catch (IllegalArgumentException ex) {
			// handle excepion.
		}

	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		pool.close();
	}

}
