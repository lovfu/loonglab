package org.ictclas4j.segment;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ictclas4j.bean.SegResult;
import org.ictclas4j.utility.Utility;
import org.loonglab.MyBeanUtil;
import org.loonglab.segment.dictionary.WordItem;
import org.loonglab.segment.util.ChineseEncoder;

public class SegTagTest extends TestCase {
	
	static Log log = LogFactory.getLog(SegTagTest.class);

	//SegTag segTag = new SegTag("dic");
	
	public void testSplit() throws Exception {
		
		BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream("segtest.txt"),"GBK"));
		StringBuffer text=new StringBuffer();
		String line=reader.readLine();
		while(line!=null){
			text.append(line);
			line=reader.readLine();
		}
		reader.close();
		
		//SegResult result=segTag.split(text.toString());
		
		//log.info(result.getFinalResult());
	}
	
	public void testUtility() throws Exception {
		//log.info("===="+Utility.CC_ID("/"));
		log.info("====="+ChineseEncoder.hashCode('��'));
	}
	
	public void testClone(){
		WordItem i1=new WordItem("haha");
		WordItem i2=(WordItem) i1.clone();
		//i2=i1;
		i2.setWord("xixi");
		
		log.info(i1.getWord()+","+i2.getWord());
	}

}
