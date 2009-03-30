package org.loonglab.segment.dictionary.impl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ictclas4j.utility.Utility;
import org.loonglab.segment.dictionary.Dictionary;
import org.loonglab.segment.dictionary.Hit;
import org.loonglab.segment.dictionary.WordItem;


public class HashBinaryDictionary implements Dictionary{
	
	static Log log = LogFactory.getLog(HashBinaryDictionary.class);
	
	/**
	 * ����hash������ʣ�ಿ���ö��ַ�����
	 */
	public Map<Integer,List<WordItem>> wts=new HashMap<Integer, List<WordItem>>();
	
	private Hit lastHit=null;
	private int lastStart=-1;
	private int lastEnd=-1;



	/**
	 * ��ȡ�ʵ��ļ�
	 * @param filename
	 */
	public HashBinaryDictionary(String filename) {
		loadDic(filename);
	}
	
	public void loadDic(String filename){
		try {
			
			BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(filename),"GBK"));

			String wordStr;
			while((wordStr=br.readLine())!=null){
				String[] words=wordStr.split(" ");
				String word=words[0].substring(1);
				
				//����key
				//log.debug("===word is "+word);
				int key=new Integer(words[0].charAt(0));
				
				WordItem wi=new WordItem(word,new Integer(words[2]),new Integer(words[1]));
				List<WordItem> wis=wts.get(key);
				if(wis==null){
					wis=new ArrayList<WordItem>();
					wts.put(key, wis);
				}
				wis.add(wi);
			}
					
			

			
			br.close();
			
		} catch (Exception e1) {
			log.error(e1.getMessage(),e1);
			throw new RuntimeException(e1.getMessage(),e1);
		}

	}

	public void loadBidDic(String filename){
		
	}
	


	
	
	/**
	 * �Ƚ������ַ����Ĵ�С
	 * @param s1
	 * @param s2
	 * @return 0��ʾ�����ַ�����ȣ�1��ʾs1����s2������ǰ���ַ���ͬ�������Ȳ�ͬ��
	 *         2��ʾs1����s2��������ǰ���ַ���ͬ��-1��ʾs2>s1������ǰ���ַ���ͬ�������Ȳ�ͬ
	 *         -2��ʾs2>s1��ǰ���ַ���ͬ
	 */
	public static int compareTo(String s1, String s2) {
		int len = Math.min(s1.length(), s2.length());
		for (int i = 0; i < len; i++) {
			char c1=s1.charAt(i);
			char c2=s2.charAt(i);
			if(c1>c2)
				return 2;
			else if(c1<c2)
				return -2;
		}

		if (s1.length() > s2.length())
			return 1;
		else if (s1.length() < s2.length())
			return -1;
		else
			return 0;


	}

	/**
	 * ���ֵ�����Ҵ���
	 * @param word ����
	 * @param start ���ַ����ҿ�ʼλ�ã����start=-1,���0��ʼ����
	 * @param end ���ַ����ҵĽ���λ�ã����end=-1��������ʼ����
	 * @return
	 */
	public Hit search(String word) {
		if (word != null) {
			String res=word.substring(1);
			int index=keyOf(word.charAt(0));
			if (res != null && wts != null) {
				List<WordItem> wt = wts.get(index);
				if (wt != null && wt.size() > 0) {
					int start=0;
					int end = wt.size() - 1;
					if(lastStart!=-1)
						start = lastStart;
					if(lastEnd!=-1)
						end= lastEnd;
					int mid = 0;
					List<WordItem> wis = wt;
					
					//�����´β��ҵķ�Χ
					lastStart=start;
					lastEnd=end;
					while (start <= end) {
						mid = (start + end) / 2;
						WordItem wi = wis.get(mid);
						int cmpValue = compareTo(wi.getWord(), res);
						if (cmpValue == 0 ) {
							lastHit=new Hit(word,wi,Hit.STATE_HIT);
							//WordResult wr=new WordResult(wi,resultStart,resultEnd);
							return lastHit;

						} else if (cmpValue < 0 ){						
							start = mid + 1;
							lastStart=start;
						}
							
						else if (cmpValue > 0 ){						
							end = mid - 1;
							if(cmpValue==2)
								lastEnd=end;
						}
							

						
					}
					

					if(wis.get(mid).getWord().startsWith(res)){
						lastHit=new Hit(word,null,Hit.STATE_NEXT);
						return lastHit;
					}

					
				}
				
				
			}
		}
		
		lastHit=new Hit(word,null,Hit.STATE_NONE);
		return lastHit;

	}

    private int keyOf(char c){
    	return new Integer(c);
    }
    
    
    public static void main(String[] args) {
		Dictionary dic=new HashBinaryDictionary("dic/coreDict.dct");
		
		Hit hit=dic.search("���ķ��");
		
		
	}

}
