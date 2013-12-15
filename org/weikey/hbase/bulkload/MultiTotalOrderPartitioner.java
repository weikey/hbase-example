package org.weikey.hbase.bulkload;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;


public class MultiTotalOrderPartitioner extends TotalOrderPartitioner<ImmutableBytesWritable, Put>{

	public int getPartition(ImmutableBytesWritable key, Put value, int numPartitions) {
		byte[] keyBytes = key.get();
		ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.tail(keyBytes, getRowKeyLength(keyBytes)));
	    return super.getPartition(rowKey, value, numPartitions);
	  }
	
	private int getRowKeyLength(byte[] keyBytes){		
		return keyBytes.length - Util.getNameLength(keyBytes);
	}
}
