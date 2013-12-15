package org.weikey.hbase.bulkload;

public interface Constants {
	String TABLE_ROWKEY_DELIMITER = String.valueOf((char)26);
	
	String FIELD_DELIMITER = String.valueOf((char) 27);
	String ROWKEY_END = String.valueOf((char) 28);	
	String ROWKEY_END_NEXT = String.valueOf((char) 29);
	String FUNC_SUBSTRING = "substring";
}
