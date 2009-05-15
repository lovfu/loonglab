package org.loonglab.segment.dictionary;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

/**
 * ���Ա��
 * 
 */
public class POS {
	private int tag;// ���Ա�ǣ��磺nr����8λ������Եĵ�һ���ַ�����8λ����ʵĵڶ����ַ�

	private int freq;// �ô��Գ��ֵ�Ƶ��
	
	//private int prev;//ǰһ���ʵ�N�������к͸ô�����ƥ�����һ�����±�λ�ã�
	
	private boolean isBest;
	
	public  POS(){
		
	}
	
	public POS(int pos,int value){
		this.tag=pos;
		this.freq=value;
	}
	
	public int getTag() {
		return tag;
	}

	public void setTag(int pos) {
		this.tag = pos;
	}

	public int getFreq() {
		return freq;
	}

	public void setFreq(int value) {
		this.freq = value;
	}
  
	public boolean isBest() {
		return isBest;
	}

	public void setBest(boolean isBest) {
		this.isBest = isBest;
	}

	public String toString() {

		return ReflectionToStringBuilder.toString(this);

	}
}
