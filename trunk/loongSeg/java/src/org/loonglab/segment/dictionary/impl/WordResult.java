package org.loonglab.segment.dictionary.impl;

import org.loonglab.segment.dictionary.WordItem;

/**
 * ����ʵ������Ľ��
 * @author loonglab
 *
 */
public class WordResult {
	
	/**
	 * ��ѯ�Ĵ���
	 */
	String searchWord;
	
	/**
	 * �����Ľ�������Ϊnull�����ʾû���������ô�
	 */
	WordItem wordItem;
	
	/**
	 * �����Ըôʿ�ͷ�Ĵ�ʱ����wordtable���ö��ַ�����ʱ��ʼ������ֵ
	 * �����ֵΪ-1����ʾû���Ըôʿ�ͷ�Ĵʣ�����Ҫ��������
	 */
	int start;
	
	/**
	 * �����Ըôʿ�ͷ�Ĵ�ʱ����wordtable���ö��ַ�����ʱ����������ֵ
	 */
	int end;

	public WordItem getWordItem() {
		return wordItem;
	}

	public void setWordItem(WordItem wordItem) {
		this.wordItem = wordItem;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
	}

	public WordResult(WordItem wordItem, int start, int end) {
		super();
		this.wordItem = wordItem;
		this.start = start;
		this.end = end;
	}

	public String getSearchWord() {
		return searchWord;
	}

	public void setSearchWord(String searchWord) {
		this.searchWord = searchWord;
	}
	
	
	
	
}
