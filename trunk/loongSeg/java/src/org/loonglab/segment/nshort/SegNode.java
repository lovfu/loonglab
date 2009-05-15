package org.loonglab.segment.nshort;

import java.util.ArrayList;
import java.util.List;

import org.loonglab.segment.dictionary.WordItem;

public class SegNode {
	
	/**
	 * ��¼��СȨֵ
	 */
	double pathValue;
	
	/**
	 * ��¼��Ӧ��СȨֵ�ĸ��ڵ�
	 */
	SegNode pathParent;
	
	/**
	 * �ýڵ���ӽڵ�ִʵĿ�ʼλ��
	 */
	int nextIndex;
	
	/**
	 * �����Ĵ���
	 */
	WordItem wordItem;
	
	/**
	 * ָ�����һ���ڵ�
	 */
	List<SegLink> links=new ArrayList<SegLink>();
	
	
	

	public int getNextIndex() {
		return nextIndex;
	}

	public void setNextIndex(int nextIndex) {
		this.nextIndex = nextIndex;
	}

	public WordItem getWordItem() {
		return wordItem;
	}

	public void setWordItem(WordItem wordItem) {
		this.wordItem = wordItem;
	}

	public List<SegLink> getLinks() {
		return links;
	}

	public void setLinks(List<SegLink> links) {
		this.links = links;
	}
	
	public void addLink(SegLink link){
		links.add(link);
	}

	public double getPathValue() {
		return pathValue;
	}

	public void setPathValue(double pathValue) {
		this.pathValue = pathValue;
	}

	public SegNode getPathParent() {
		return pathParent;
	}

	public void setPathParent(SegNode pathParent) {
		this.pathParent = pathParent;
	}
	
	@Override
	public String toString() {
		return wordItem.getWord();
	}
	
	
}
