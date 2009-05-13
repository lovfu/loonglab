package org.loonglab.segment.dictionary.trie;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.loonglab.segment.dictionary.WordItem;

/**
 * Trie���ڵ㣬ÿ���ڵ��Ӧһ���ַ�
 * @author loonglab
 *
 */
public class TrieNode implements Comparable<TrieNode>{
	
	/**
	 * �ӽڵ��б���СΪ���ʾû���ӽڵ�
	 */
	List<TrieNode> subNodes=new ArrayList<TrieNode>();
	
	/**
	 * ����ýڵ�Ϊĳ�����ĩ�ڵ㣬������Ա�ʾ��Ӧ�Ĵ�����������Ϊnull
	 */
	WordItem wordItem;
	
	/**
	 * �ýڵ��Ӧ���ַ�
	 */
	char ch;
	
	/**
	 * �ڼ���
	 */
	int layer;
	
	

	public int getLayer() {
		return layer;
	}

	public void setLayer(int layer) {
		this.layer = layer;
	}

	public List<TrieNode> getSubNodes() {
		return subNodes;
	}

	public void setSubNodes(List<TrieNode> subNodes) {
		this.subNodes = subNodes;
	}

	public WordItem getWordItem() {
		return wordItem;
	}

	public void setWordItem(WordItem wordItem) {
		this.wordItem = wordItem;
	}

	public char getCh() {
		return ch;
	}

	public void setCh(char ch) {
		this.ch = ch;
	}

	public int compareTo(TrieNode o) {
		
		return ch-o.getCh();
	}
	
	public void addSubNode(TrieNode tn){
		subNodes.add(tn);
		Collections.sort(subNodes);
	}
	
	@Override
	public String toString() {		
		return ReflectionToStringBuilder.toString(this);
	}
	
	
	
}
