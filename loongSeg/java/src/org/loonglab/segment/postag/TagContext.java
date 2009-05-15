package org.loonglab.segment.postag;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

/**
 * ���Զ�Ԫͳ��
 * @author loonglab
 *
 */
public class TagContext {
	/**
	 * ֧�ֶ����Ԫͳ��ģ�ͣ�Ŀǰ����
	 */
	private int key;

	/**
	 * ���Զ�Ԫ��Ƶͳ��
	 */
	private int[][] contextArray;

	/**
	 * �������Ե�Ƶ��
	 */
	private int[] tagFreq;

	/**
	 * ���д��Ե�Ƶ���ܺ�
	 */
	private int totalFreq;

	public int[][] getContextArray() {
		return contextArray;
	}

	public void setContextArray(int[][] contextArray) {
		this.contextArray = contextArray;
	}

 
	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public int[] getTagFreq() {
		return tagFreq;
	}

	public void setTagFreq(int[] tagFreq) {
		this.tagFreq = tagFreq;
	}

	public int getTotalFreq() {
		return totalFreq;
	}

	public void setTotalFreq(int totalFreq) {
		this.totalFreq = totalFreq;
	}
	public String toString() {

		return ReflectionToStringBuilder.toString(this);

	}
}
