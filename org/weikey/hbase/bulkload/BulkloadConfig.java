package org.weikey.hbase.bulkload;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

public class BulkloadConfig {
	class LoadInformation{
		String tableName;
		String inputPath;
		String tableStruct;
		String rowkeyCols;
		String loadCols;
		String fieldDelimiter;
		String spilts[];
		boolean appendUuid = false;
		boolean put = false;
		List<String> indexs = new ArrayList<String>();
	}
	
	private List<LoadInformation> loadInformations = new ArrayList<LoadInformation>();
	private int threadNum;
	private int step = 0;	//0 none; 1 system; 2 table
	
	public List<LoadInformation> getLoadInformations(){
		return loadInformations;
	}
	
	public int getThreadNum(){
		return threadNum;
	}
	
	public boolean parseConfigFile(String configFileName) throws IOException {
		BufferedReader reader = null;
		try{
			reader = new BufferedReader(new FileReader(configFileName));
			String line = null, tmp = null;
	
			while ((tmp=reader.readLine()) != null){
				tmp = tmp.trim();
				if (line == null){
					line = tmp;
				}else{
					line += tmp;
				}
	
				if (line.length() == 0){
					continue;
				}
				if (line.length() > 1 && line.substring(0, 2).equals("//")){	//注释行
					line = "";
					continue;
				}
				if (line.charAt(0) == '[' && line.charAt(line.length() - 1) == ']'){
					if (line.equals("[system]")){
						step = 1;
					}else if (line.equals("[table]")){
						step = 2;
					}else{
						step = 0;
					}
					line = "";
					continue;
				}
							
				if (step == 1){
					processSystemLine(line);
					line = "";
				}else if (step == 2){
					if (line.charAt(line.length() - 1) == ';'){
						line = line.substring(0, line.length() - 1);	//去掉';'
						processTableLine(line);
						line = "";
					}
				}else{
					line = "";
				}
			}
		}finally{
			reader.close();
		}
		
		return true;
	}
	
	private void processSystemLine(String line){
		if (line.substring(0, 10).equals("threadnum=")){
			threadNum = Integer.parseInt(line.substring(10).trim());
			line = "";
		}
	}
	
	private void processTableLine(String line){
		//tablename###inputpath###tablestruct###loadcols
		//###rowkeyCols###fieldDelimiter###splitregion###appenduuid(true,false)###put(true,false)
		String fields[] = line.split("###");	//配置分隔符
		
		if (fields.length < 2){
			return;
		}
		
		if (fields[0].charAt(0) == '@'){	//索引设置
			String tableName = fields[0].substring(1);
			LoadInformation loadInformation = getLoadInformation(tableName);
			if (loadInformation.tableName == null){	//新表名
				loadInformation.tableName = tableName;
				loadInformations.add(loadInformation);
			}
			if (loadInformation.indexs == null){
				loadInformation.indexs = new ArrayList<String>();
			}
			loadInformation.indexs.add(fields[1]);
		}else{	//表设置
			for (int i=0; i<fields.length; i++){
				fields[i] = fields[i].trim();
			}
			System.out.println(fields.length);
			if (fields.length == 6){
				LoadInformation e = new LoadInformation();
				e.tableName = fields[0];
				e.inputPath = fields[1];
				e.tableStruct = fields[2];
				e.loadCols = fields[3];
				e.rowkeyCols = fields[4];
				e.fieldDelimiter = fields[5];
				loadInformations.add(e);
			}else if (fields.length == 7){
				LoadInformation e = new LoadInformation();
				e.tableName = fields[0];
				e.inputPath = fields[1];
				e.tableStruct = fields[2];
				e.loadCols = fields[3];
				e.rowkeyCols = fields[4];
				e.fieldDelimiter = fields[5];
				e.spilts = fields[6].split(",");
				loadInformations.add(e);
			}else if (fields.length == 8){
				LoadInformation e = new LoadInformation();
				e.tableName = fields[0];
				e.inputPath = fields[1];
				e.tableStruct = fields[2];
				e.loadCols = fields[3];
				e.rowkeyCols = fields[4];
				e.fieldDelimiter = fields[5];
				e.spilts = fields[6].split(",");
				e.appendUuid = Boolean.parseBoolean(fields[7]);
				loadInformations.add(e);
			}else if (fields.length == 9){
				LoadInformation e = new LoadInformation();
				e.tableName = fields[0];
				e.inputPath = fields[1];
				e.tableStruct = fields[2];
				e.loadCols = fields[3];
				e.rowkeyCols = fields[4];
				e.fieldDelimiter = fields[5];
				e.spilts = fields[6].split(",");
				e.appendUuid = Boolean.parseBoolean(fields[7]);
				e.put = Boolean.parseBoolean(fields[8]);
				loadInformations.add(e);
			}else{
				System.err.println("error config line:" + line);
			}
		}
	}
	
	private LoadInformation getLoadInformation(String tableName){
		for (Iterator<LoadInformation> iter=loadInformations.iterator(); iter.hasNext(); ){
			LoadInformation loadInformation = iter.next();
			if (loadInformation.tableName.equals(tableName)){
				return loadInformation;
			}
		}
		
		return new LoadInformation();
	}
}
