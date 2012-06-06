package org.loonglab.hdc.imp;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

/**
 * 读取数据中心标准格式数据文件，数据文件标准格式：
 * （1）csv类似格式，第一行为字段名，从第二行开始为数据
 * （2）字段名和字段值的分隔符为tab符
 * （3）空字符用"-"表示，读入到内存则设置为null
 * （4）字符集为GBK
 * @author liugang
 *
 */
public class DataReader {
	//private static Log log = LogFactory.getLog(DataReader.class);
	
	String fileName;
	//csv文件的头
	String[] header;
		
	LineIterator lineIterator;
	
	
	public String[] getHeader() {
		return header;
	}
	
	public Map<String, Integer> getHeaderMap(){
		Map<String, Integer> headerMap=new HashMap<String, Integer>();
        for (int i = 0; i < header.length; i++) {
            headerMap.put(header[i], i);
        }
        
        return headerMap;
	}
	
	

	
    public String getFileName() {
    	return fileName;
    }

	public DataReader(String fileName) throws IOException{	
		this.fileName=fileName;
		File file=new File(fileName);
		lineIterator=FileUtils.lineIterator(file, DataConfig.CHARSET);
		
		//初始化头
		if(lineIterator.hasNext()){
			String headLine=lineIterator.nextLine();
			header=headLine.split(DataConfig.SEPARATE_TAG);
			
		}

	}

	public boolean hasNext() {		
		return lineIterator.hasNext();
	}


	public String[] next() {
		
		String line=lineIterator.nextLine();
		String[] result = line.split(DataConfig.SEPARATE_TAG);
		for (int i = 0; i < result.length; i++) {
	        String value = result[i];
	        if(value.equals("-"))
	        	result[i]=null;
        }
		
		return result;
		
	}

	
	public void close(){
		lineIterator.close();
	}
}
