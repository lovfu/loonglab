package org.loonglab.segment.dictionary;

/**
 * �ֵ�ӿ�
 * @author loonglab
 *
 */
public interface Dictionary {
	/**
	 * ���ֵ��в�ѯĳ����
	 * @param word
	 * @return
	 */
	public Hit search(String word);
	
}
