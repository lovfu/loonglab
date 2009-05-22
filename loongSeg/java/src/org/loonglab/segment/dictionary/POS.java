package org.loonglab.segment.dictionary;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

/**
 * ���Ա��
 * 
 */
public class POS implements Cloneable{
	private int tag;// ���Ա�ǣ��磺nr����8λ������Եĵ�һ���ַ�����8λ����ʵĵڶ����ַ�

	private int freq;// �ô��Գ��ֵ�Ƶ��
	
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

	@Override
	public Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return super.clone();
	}
	
	
	
}
