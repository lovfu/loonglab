package org.loonglab.hdc.imp;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.loonglab.hdc.common.HTablePoolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImpFileHandler implements Callable<String> {
	
	private static Logger log=LoggerFactory.getLogger(ImpFileHandler.class);

	private File joinFile;
	
	
	public ImpFileHandler(File joinFile) {
		super();
		this.joinFile = joinFile;
	}



	@Override
	public String call() throws Exception {		
		DataReader reader=null;
		HTableInterface roiTable=null;
        try {
        	roiTable=HTablePoolFactory.getPool().getTable("roi");
        	List<Put> putList=new ArrayList<Put>();
			reader=new DataReader(joinFile);
			String[] headers=reader.getHeader();   
			int i=0;
			if(reader.hasNext()){
				i++;
				String[] values=reader.next();
				String key=getBaseKey(joinFile)+"_"+i;
				Put put=new Put(Bytes.toBytes(key));
				for (int j = 0; j < headers.length; j++) {
					if(values[j]!=null){
						put.add(Bytes.toBytes("cf"), Bytes.toBytes(headers[j]), Bytes.toBytes(values[j]));
						putList.add(put);
					}
					
					if(putList.size()>100){
						roiTable.put(putList);
						putList.clear();
					}
				}
				
			}
			
			return joinFile.getCanonicalPath();
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		} finally{
			if(reader!=null)
				reader.close();
			if(roiTable!=null)
				roiTable.close();
		}
		return null;
	}
	
	private String getBaseKey(File file){
		//File joinFile=new File(fileName);
		String tenantId=joinFile.getName().substring(0, joinFile.getName().length()-4);
		String type=joinFile.getParentFile().getParentFile().getName();
		String statDate=joinFile.getParentFile().getName();
		String searchEngine=joinFile.getParentFile().getParentFile().getParentFile().getName();
		
		String key=tenantId+"_"+searchEngine+"_"+type+"_"+statDate;
		
		return key;
	}

}
