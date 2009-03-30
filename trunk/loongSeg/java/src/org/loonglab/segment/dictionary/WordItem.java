package org.loonglab.segment.dictionary;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

/**
 * ����.���������ݡ����ȡ������Ƶ��
 * ������ͬ����
 * 
 */
public class WordItem {
	private String word;
	private int handle;// �����������ʶ�ʵĴ���
	private int freq;// Ƶ�ȣ�����˵���ôʳ��������Ͽ��еĴ��������

	
	Dictionary biDic;//��Ԫ�����ʵ�
	
	
	public WordItem(String _word,int _handle,int _freq) {
		word=_word;
		handle=_handle;
		freq=_freq;
	}

	
	
	public int getFreq() {
		return freq;
	}

	public void setFreq(int frequency) {
		this.freq = frequency;
	}

	public int getHandle() {
		return handle;
	}

	public void setHandle(int handle) {
		this.handle = handle;
	}



	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}
	
	
	public String toString() {

		return ReflectionToStringBuilder.toString(this);

	}


	
	
	 

}
