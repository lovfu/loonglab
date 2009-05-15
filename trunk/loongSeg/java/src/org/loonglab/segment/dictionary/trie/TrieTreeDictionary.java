package org.loonglab.segment.dictionary.trie;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.loonglab.segment.SegmentException;
import org.loonglab.segment.dictionary.WordItem;
import org.loonglab.segment.util.ChineseEncoder;

public class TrieTreeDictionary {
	
	static Log log = LogFactory.getLog(TrieTreeDictionary.class);
	
	TrieNode[] dicWords=new TrieNode[7000];
	
	public TrieTreeDictionary(WordItem[] wordItems) {
		buildTrieTree(wordItems);
	}
	
	public TrieTreeDictionary(List<WordItem> wordItems) {
		buildTrieTree(wordItems.toArray(new WordItem[wordItems.size()]));
	}
	
	
	
	private void buildTrieTree(WordItem[] wordItems) {
		for (WordItem wordItem : wordItems) {
			addWordItem(wordItem);
			
		}
		
	}

	private void addWordItem(WordItem wordItem) {
		
		//TODO ��ͬ�ʣ���ͬ���Եĵ���
		String wordStr=wordItem.getWord();
		
		TrieNode commonNode=search(wordStr);
		
		if(commonNode==null){
			//���ֲ�����
			char firstChar=wordStr.charAt(0);
			int firstkey=ChineseEncoder.hashCode(firstChar);
			
			TrieNode tn=new TrieNode();
			tn.setCh(firstChar);
			tn.setLayer(0);
			
			dicWords[firstkey]=tn;
			
			if(wordStr.length()==1){
				tn.setWordItem(wordItem);
				return;
			}
			else
				commonNode=tn;
		}
		
		for (int i = commonNode.getLayer()+1; i < wordStr.length(); i++) {
			char c=wordStr.charAt(i);
			TrieNode tn=new TrieNode();
			tn.setCh(c);
			tn.setLayer(i);
			
			commonNode.addSubNode(tn);
			
			if(i==wordStr.length()-1){
				tn.setWordItem(wordItem);				
			}
			
			commonNode=tn;
			
		}

	}



	/**
	 * ����ĳ������trie���е�λ�ã������Ѵ��ڵĹ���ǰ׺�����һ���ڵ㡣����null��ʾ�����ڴʵ���Ҳ������
	 * @param word
	 * @return
	 */
	private TrieNode search(String word){
		char firstChar=word.charAt(0);
		int firstKey=ChineseEncoder.hashCode(firstChar);
		
		TrieNode tn=dicWords[firstKey];
		
		if(tn!=null){
			if(word.length()==1)
				return tn;
			
			for (int i = 1; i < word.length(); i++) {
				char c=word.charAt(i);
				TrieNode node=binarySearch(tn.subNodes, c);
				if(node==null){
					return tn;
				}
				else if(i==word.length()-1){
					return node;
				}
				else
					tn=node;
			}
		}
		
		return null;
	}
	
	/**
	 * ����ĳһ�����ʣ����û�в鵽����null
	 * @param word
	 * @return
	 */
	public WordItem searchWord(String word){
		char firstChar=word.charAt(0);
		int firstKey=ChineseEncoder.hashCode(firstChar);
		
		TrieNode tn=dicWords[firstKey];
		
		if(tn!=null){
			if(word.length()==1)
				return tn.getWordItem();
			
			for (int i = 1; i < word.length(); i++) {
				char c=word.charAt(i);
				TrieNode node=binarySearch(tn.subNodes, c);
				if(node==null){
					return null;
				}
				else if(i==word.length()-1){
					return node.getWordItem();
				}
				else
					tn=node;
			}
		}
		
		return null;
	}
	
	/**
	 * �ö��ַ����ӽڵ��б��в�ѯ��Ӧĳ���ַ��Ľڵ�,����null��ʾû���ҵ�
	 * @param subNodes
	 * @param c
	 * @return
	 */
	private TrieNode binarySearch(List<TrieNode> subNodes,char c){
		int start=0;
		int end = subNodes.size() - 1;
		int mid = 0;
		
		while (start <= end) {
			mid = (start + end) / 2;
			TrieNode tn = subNodes.get(mid);
			int cmpValue = tn.getCh()-c;
			if (cmpValue == 0 ) {
				return tn;

			} else if (cmpValue < 0 ){						
				start = mid + 1;						
			}						
			else if (cmpValue > 0 ){						
				end = mid - 1;						
			}

		}
		
		return null;
				

	}

	/**
	 * ȫ�з�
	 * @param sentence
	 * @return
	 */
	public List<WordItem> allSplit(String sentence){
		List<WordItem> resultList=new ArrayList<WordItem>();
		
		if(sentence!=null&&sentence.length()!=0){
			char firstChar=sentence.charAt(0);
			int firstKey=ChineseEncoder.hashCode(firstChar);
			
			TrieNode tn=dicWords[firstKey];
			if(tn!=null){
				if(tn.getWordItem()!=null)
					resultList.add(tn.getWordItem());
				
				for (int i = 1; i < sentence.length(); i++) {
					char c=sentence.charAt(i);
					TrieNode subNode=binarySearch(tn.getSubNodes(), c);
					if(subNode==null)
						break;
					
					if(subNode.getWordItem()!=null)
						resultList.add(subNode.getWordItem());
					
					tn=subNode;
				}
				
			}
		}
		
		return resultList;
	}
	
	public void print(){
		for (int i = 0; i < dicWords.length; i++) {
			TrieNode tn=dicWords[i];
			if(tn!=null){
				System.out.println(tn);
			}
		}
	}
	
	public static void main(String[] args) {
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
			
			long startTime=System.currentTimeMillis();
			
			TrieTreeDictionary ttd=new TrieTreeDictionary(allWords);
			
			log.info("cost time is "+(System.currentTimeMillis()-startTime));
			
			String sentence="��˵��abcȷʵ����";
			
			for (int i = 0; i < sentence.length(); i++) {
				String subSen=sentence.substring(i);
				//log.info(subSen);
				//log.info(sentence.subSequence(i, i+1));
				
				List<WordItem> results=ttd.allSplit(subSen);
				
				for (WordItem wordItem : results) {
					log.info(wordItem);
				}
				
				if(results.size()==0)
					log.info(sentence.subSequence(i, i+1));
			}
			
			
			//ttd.print();

			
		} catch (Exception e1) {
			log.error(e1.getMessage(),e1);
			throw new SegmentException(e1.getMessage(),e1);
		}
	}
}
