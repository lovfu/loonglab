package org.ictclas4j.bean;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ictclas4j.utility.GFString;
import org.ictclas4j.utility.SegException;
import org.ictclas4j.utility.Utility;

/**
 * �ı���ʽ���ֵ䣬��֧�ִӳ������޸��ֵ�
 * @author loonglab
 *
 */
public class TxtDictionary extends Dictionary {
	
	static Log log = LogFactory.getLog(TxtDictionary.class);
	
	

	public TxtDictionary() {
		super();		
	}



	public TxtDictionary(String filename) {
		super(filename);		
	}



	@Override
	public boolean load(String filename, boolean isReset) {
		
		try {
			//���ݾɰ汾
			delModified();
			
			BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(filename),"GBK"));
			for (int i = 0; i < Utility.CC_NUM; i++) {
				String countStr=br.readLine();
				//System.out.println(countStr);
				int count=new Integer(countStr);
				wts.get(i).setCount(count);				
				if(count>0){
					WordItem[] wis = new WordItem[count];
					for (int j = 0; j < count; j++) {
						String wordStr=br.readLine();						
						String[] words=wordStr.split(" ");
						String word=words[0].substring(1);
						WordItem wi=new WordItem(word,word.length()+1,new Integer(words[2]),new Integer(words[1]),j);
//						if(wordStr.startsWith("ϲ��"))
//							System.out.println(wi);
						wis[j]=wi;
					}
					
					wts.get(i).setWords(wis);
				}
			}
			
			br.close();
			
		} catch (Exception e1) {
			log.error(e1.getMessage(),e1);
			return false;
		}

		return true;
	}
	
	/**
	 * �ں��Ĵʵ��е����ԪƵ�ȴʵ䣬��߲�ѯ�ٶ�
	 * @param filename
	 */
	public void loadBiDict(String filename){
		try {
			BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(filename),"GBK"));
			
			String wordStr;
			WordItem lastItem=null;
			while((wordStr=br.readLine())!=null){
				//log.debug(wordStr);
				String[] wordArray=wordStr.split(" ");
				if(wordArray.length<3)
					continue;
				String biWordStr=wordArray[0];
				int freq=new Integer(wordArray[1]);
				int handle=new Integer(wordArray[2]);
				
				String[] bidWordArray=biWordStr.split("@");
				String firstWord=bidWordArray[0];
				String secondWord=bidWordArray[1];
				String firstWordHead=firstWord.substring(0,1);
				String firstWordTail=firstWord.substring(1);
				
				//�ں��Ĵʵ��в�����Ӧ�Ĵ�
				if(lastItem==null||!lastItem.getWord().equals(firstWordTail)){
					int found=super.findInOriginalTable4Split(Utility.CC_ID(firstWordHead), firstWordTail, 0);
					WordItem wi=wts.get(Utility.CC_ID(firstWordHead)).getWords().get(found);
					if(wi.getWord().equals(firstWordTail)){
						lastItem=wi;
					}
				}
				
				if(lastItem!=null){
					Map<String, BiWordItem> biMap=lastItem.getBiDic();
					if(biMap==null){
						biMap=new HashMap<String, BiWordItem>();
						lastItem.setBiDic(biMap);
					}
					
						
					
					BiWordItem bwi=new BiWordItem();
					bwi.setWord(secondWord);
					bwi.setFreq(freq);
					bwi.setHandle(handle);
					
					biMap.put(bwi.getWord(), bwi);
				}
			}
			
			br.close();
			
			
			
		} catch (Exception e) {
			log.error(e.getMessage(),e);
			throw new SegException(e.getMessage(),e);
		}
	}
	
	@Override
	public void loadUserDict(String filename){
		//log.debug("enter "+filename+" loadUserDict...");
		try {
			BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(filename),"GBK"));
			
			String wordStr;
			while((wordStr=br.readLine())!=null){
				//log.debug(wordStr);
				String[] wordArray=wordStr.split(" ");
				String wordHead=wordArray[0].substring(0,1);
				String wordTail=wordArray[0].substring(1);
				int id=Utility.CC_ID(wordHead);
				WordTable wt=wts.get(id);
				if(wt!=null){
					if(wt.getCount()==0){
						wt.setWords(new ArrayList<WordItem>());						
					}
					WordItem wi=new WordItem(wordTail,wordTail.length()+1,new Integer(wordArray[2]),new Integer(wordArray[1]));
					wt.getWords().add(wi);
					wt.setCount(wt.getCount()+1);
					
					//log.debug(wi);
					
					Collections.sort(wt.getWords(), new Comparator<WordItem>(){

						public int compare(WordItem o1, WordItem o2) {
							return GFString.compareTo(o1.getWord(), o2.getWord());
						}
						
					});
					
					for (int i = 0; i < wt.getWords().size(); i++) {
						WordItem item=wt.getWords().get(i);
						item.setIndex(i);
					}
					
				}
			}
			
			br.close();
			
			
			
		} catch (Exception e) {
			log.error(e.getMessage(),e);
			throw new SegException(e.getMessage(),e);
		}
		
	}
	
}
