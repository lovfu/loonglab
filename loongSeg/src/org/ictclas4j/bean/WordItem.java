package org.ictclas4j.bean;

import java.util.Map;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

/**
 * ����.���������ݡ����ȡ������Ƶ��
 * 
 * @author sinboy
 * 
 */
public class WordItem {
	private String word;
	private int len;//���ڴ����ϴ洢���ֽڳ���
	private int handle;// �����������ʶ�ʵĴ���
	private int freq;// Ƶ�ȣ�����˵���ôʳ��������Ͽ��еĴ��������
	
	private int index;//��wordTable���������
	
	/**
	 * ʹ��jdk�Դ���HashMapʵ�֣���Ϊ��������С��Ӧ���ܱ�֤����
	 * �������޷����㣬�ɿ�����������
	 */
	private Map<String,BiWordItem> biDic;//�Ը�wordItemΪ��ͷ�Ķ�Ԫ�����б�
	
	


	public Map<String, BiWordItem> getBiDic() {
		return biDic;
	}

	public void setBiDic(Map<String, BiWordItem> biDic) {
		this.biDic = biDic;
	}

	WordItem() {
		
	}
	
	WordItem(String _word,int _len,int _handle,int _freq) {
		word=_word;
		len=_len;
		handle=_handle;
		freq=_freq;
	}
	
	WordItem(String _word,int _len,int _handle,int _freq,int _index) {
		word=_word;
		len=_len;
		handle=_handle;
		freq=_freq;
		index=_index;
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

	public int getLen() {
		return len;
	}

	public void setLen(int len) {
		this.len = len;
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

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}
	
	
	 

}
