package org.loonglab.segment.nshort;

/**
 * ��֮�������
 * @author loonglab
 *
 */
public class SegLink {
	/**
	 * Ȩֵ
	 */
	double value;
	
	/**
	 * ָ���Ŀ��ڵ�
	 */
	SegNode target;

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public SegNode getTarget() {
		return target;
	}

	public void setTarget(SegNode target) {
		this.target = target;
	}

	public SegLink(double value, SegNode target) {
		super();
		this.value = value;
		this.target = target;
	}
	
	
}
