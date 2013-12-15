package org.weikey.hbase.bulkload;

import java.math.BigInteger;
import java.net.InetAddress;  
import java.net.UnknownHostException;  
import java.security.SecureRandom;
import java.util.UUID;

import org.apache.hadoop.hbase.util.Bytes;

public class Util {
    public static SecureRandom ng = new SecureRandom();
	public static String NULL = new String();
	private static String hostname;
	public static String getHostName(){
		try{
			if (hostname == null){
				hostname = InetAddress.getLocalHost().getHostName();
				if (hostname == null){
					hostname = "";
				}
			}
            return hostname; 
        }catch(UnknownHostException e){  
            System.out.println("unknown host!");
            hostname = "";
            return "";
        }
	}
	
	public static String getRowkeyId(){
		/*
        byte[] randomBytes = new byte[16];
        ng.nextBytes(randomBytes);
        return new String(randomBytes);
        */
		//String id = getHostName();
		//Long millis =  System.currentTimeMillis();
		//id += millis.toString();
		//return id;
		//UUID.randomUUID().
		
		return UUID.randomUUID().toString();
		//return NULL;
	}
	
	public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
		  byte[][] splits = new byte[numRegions-1][];
		  BigInteger lowestKey = new BigInteger(startKey, 16);
		  BigInteger highestKey = new BigInteger(endKey, 16);
		  BigInteger range = highestKey.subtract(lowestKey);
		  BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
		  lowestKey = lowestKey.add(regionIncrement);
		  for(int i=0; i < numRegions-1;i++) {
		    BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
		    byte[] b = String.format("%016x", key).getBytes();
		    splits[i] = b;
		  }
		  return splits;
		}
	
	public static byte[][] getSplits(String regions[]) {
		
		  byte[][] splits = new byte[regions.length][];
		  
		  for(int i=0; i < regions.length; i++) {
		    splits[i] = Bytes.toBytes(regions[i]);
		  }
		  
		  return splits;
		}
	
	public static String getDir(byte[] keyBytes){
		String rowkey = new String(keyBytes); 
	    return rowkey.substring(0, rowkey.indexOf(Constants.TABLE_ROWKEY_DELIMITER));
	 }
	  
	 public static int getNameLength(byte[] keyBytes){
		 String rowkey = new String(keyBytes);
		 int length = rowkey.indexOf(Constants.TABLE_ROWKEY_DELIMITER);
		 length = length == -1 ? 0 : length;
		 return length;
	}
	
}
