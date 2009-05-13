package org.loonglab.segment.nshort;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.loonglab.segment.POSTag;
import org.loonglab.segment.SegmentException;
import org.loonglab.segment.dictionary.WordItem;
import org.loonglab.segment.dictionary.trie.TrieTreeDictionary;

import com.sun.jmx.remote.internal.ArrayQueue;

public class NShortSeg {
	
	static Log log = LogFactory.getLog(NShortSeg.class);
	
	public static WordItem SENTENCE_BEGIN=new WordItem("ʼ##ʼ",1,50610);
	public static WordItem SENTENCE_END=new WordItem("ĩ##ĩ",4,50610);
	
	TrieTreeDictionary dic;
	
	
	
	public NShortSeg() {
		List<WordItem> allWords=new ArrayList<WordItem>();
		
		try {
			//1�����ʵ��ļ���������
			BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream("dic/coreDict.dct"),"GBK"));

			String wordStr;
			int maxWordLength=0;
			WordItem lastWordItem=null;
			while((wordStr=br.readLine())!=null){
				String[] words=wordStr.split(" ");
				//ע�⣬����Ҫ���ֵ��ļ����ź����
				if(lastWordItem!=null&&lastWordItem.getWord().equals(words[0])){					
					lastWordItem.addPos(new Integer(words[2]),new Integer(words[1]));
				}
				else{
					WordItem wi=new WordItem(words[0],new Integer(words[2]),new Integer(words[1]));
					allWords.add(wi);
					lastWordItem=wi;
					
					if(words[0].length()>maxWordLength)
						maxWordLength=words[0].length();
				}
				
			}
			br.close();

			
			dic=new TrieTreeDictionary(allWords);
			
			

			
		} catch (Exception e1) {
			log.error(e1.getMessage(),e1);
			throw new SegmentException(e1.getMessage(),e1);
		}
	}



	public List<WordItem> segSentence(String sentence){
		//1�������������ͼ
		
		//�ȹ����ʼ�ڵ�
		SegNode firstNode=new SegNode();
		firstNode.setPathValue(0);
		firstNode.setWordItem(SENTENCE_BEGIN);
		firstNode.setNextIndex(0);
		
		SegNode lastNode=new SegNode();
		lastNode.setPathValue(Double.MAX_VALUE);
		lastNode.setWordItem(SENTENCE_END);
		lastNode.setNextIndex(sentence.length());
		
		//��ʼ�з־���
		Queue<SegNode> nodeQueue=new LinkedList<SegNode>();
		nodeQueue.add(firstNode);
		
		while(!nodeQueue.isEmpty()){
			SegNode parentNode=nodeQueue.poll();
			if(parentNode.getNextIndex()==sentence.length()){
				parentNode.addLink(new SegLink(0,lastNode));
				
			}
			else{
				String subSen=sentence.substring(parentNode.getNextIndex());
				
				List<WordItem> results=dic.allSplit(subSen);
				
				for (WordItem wordItem : results) {
					SegNode node=new SegNode();
					node.setPathValue(Double.MAX_VALUE);
					node.setWordItem(wordItem);
					node.setNextIndex(parentNode.getNextIndex()+wordItem.getWord().length());
					parentNode.addLink(new SegLink(0,node));
					
					nodeQueue.add(node);
				}
			}
			
			
//			if(results.size()==0){
//				String wordStr=sentence.substring(i,i+1);
//				SegNode node=new SegNode();
//				node.setPathValue(Double.MAX_VALUE);
//				node.setWordItem(new WordItem(wordStr,POSTag.UNKNOWN,0));
//				
//				sn.addLink(new SegLink(0,node));
//			}
		}
		

		
		//���¸����ߵ�Ȩֵ
		print(firstNode);
		
		//2��dijkstra�㷨
		
		return null;
	}
	
	//�ݹ鷽����ӡ
	private void print(SegNode firstNode) {
		log.info(firstNode);
		for (SegLink link : firstNode.getLinks()) {
			print(link.getTarget());
		}
		
	}



	public static void main(String[] args) {
		NShortSeg nsg=new NShortSeg();
		nsg.segSentence("��˵��ȷʵ����");
	}
}
