package org.loonglab.segment.dictionary;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.loonglab.segment.util.ChineseEncoder;

/**
 * ����.���������ݡ����ȡ������Ƶ��
 * ������ͬ����
 * 
 */
public class WordItem implements Comparable<WordItem>,Cloneable{
	
	public static final String SENTENCE_BEGIN = "ʼ##ʼ";

	public static final String SENTENCE_END = "ĩ##ĩ";
	
	public static final String UNKNOWN_PERSON = "δ##��";

	public static final String UNKNOWN_SPACE = "δ##��";

	public static final String UNKNOWN_NUM = "δ##��";

	public static final String UNKNOWN_TIME = "δ##ʱ";

	public static final String UNKNOWN_LETTER = "δ##��";
	
	private String word;
	//private int handle;// �����������ʶ�ʵĴ���
	//private int freq;// Ƶ�ȣ�����˵���ôʳ��������Ͽ��еĴ��������

	private List<POS> posList=new ArrayList<POS>();
	
	/**
	 * ���ô�Ϊδ��¼��ʱ��word���Ա�ʾ�ôʵ����srcWord��ʾ�ô�ԭʼ����
	 * ��Ϊ����Ӧ��n���·���㷨ʱ����ͨ��δ��¼�����������ͬ�ָ��ʣ������ڵ�·����Ȩֵ��
	 */
	private String srcWord;
	
	//Dictionary biDic;//��Ԫ�����ʵ�
	
	
	
	public WordItem(String word){
		this.word=word;
	}
	
	public WordItem() {
		super();
		// TODO Auto-generated constructor stub
	}

	public WordItem(String _word,int _handle,int _freq) {
		word=_word;
		POS pos=new POS(_handle,_freq);
		posList.add(pos);
	}

	public List<POS> getPosList() {
		return posList;
	}

	public void setPosList(List<POS> posList) {
		this.posList = posList;
	}

	public void addPos(int handle,int freq){
		POS pos=new POS(handle,freq);
		posList.add(pos);
	}
	
	public void addPos(POS pos){
		posList.add(pos);
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}
	
	
	public String toString() {

		return ReflectionToStringBuilder.toString(this);
		//return word;

	}
	
	public int getFreq(){
		int freq=0;
		for (POS pos : posList) {
			freq=freq+pos.getFreq();
		}
		
		return freq;
	}



	public int compareTo(WordItem o) {
		String s1=this.getWord();
		String s2=o.getWord();
		
		int len = Math.min(s1.length(), s2.length());
		for (int i = 0; i < len; i++) {
			char c1=s1.charAt(i);
			char c2=s2.charAt(i);
			int key1=ChineseEncoder.hashCode(c1);
			int key2=ChineseEncoder.hashCode(c2);
			if(key1>key2)
				return 2;
			else if(key1<key2)
				return -2;
		}

		if (s1.length() > s2.length())
			return 1;
		else if (s1.length() < s2.length())
			return -1;
		else
			return 0;

	}

	public String getSrcWord() {
		return srcWord;
	}

	public void setSrcWord(String srcWord) {
		this.srcWord = srcWord;
	}

	@Override
	public Object clone() {
		WordItem item;
		try {
			item = (WordItem) super.clone();
			item.setPosList(new ArrayList<POS>());
			for (POS pos : posList) {
				item.addPos((POS)pos.clone());
			}
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e.getMessage(),e);
		}
		
		return item;
		
	}

	public POS getPos(){
		for (POS pos : posList) {
			if(pos.isBest())
				return pos;
		}
		
		return null;
	}

	
	
	
	
	 

}
