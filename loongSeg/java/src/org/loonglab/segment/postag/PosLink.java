package org.loonglab.segment.postag;

import org.loonglab.segment.dictionary.POS;

/**
 * ����viterbi�㷨���Ա�ע
 * @author loonglab
 *
 */
public class PosLink {
	
	POS pos;
	/**
	 * ǰһ���ʵ�N�������к͸ô�����ƥ�����һ�����±�λ�ã�
	 */
	int prev;
	
	/**
	 * ��ǰһ���ʵ�n�������е���ƥ�����һ����ͬ�ָ���
	 */
	double value;

	public POS getPos() {
		return pos;
	}

	public void setPos(POS pos) {
		this.pos = pos;
	}

	public int getPrev() {
		return prev;
	}

	public void setPrev(int prev) {
		this.prev = prev;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public PosLink(POS pos, double value) {
		super();
		this.pos = pos;
		this.value = value;
	}

	public PosLink() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	
	
}
