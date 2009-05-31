package org.loonglab.segment.nshort;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.loonglab.segment.dictionary.WordItem;
import org.loonglab.segment.dictionary.loader.TxtDicFileLoader;
import org.loonglab.segment.dictionary.trie.TrieTreeDictionary;

public class UnkownWordSeg {
	
	static Log log = LogFactory.getLog(UnkownWordSeg.class);
	
	protected TrieTreeDictionary unknownDict;
	
	public static int NL_LETTER=1;//��ʾ��ĸ�����ַ�
	public static int NL_NUMBER=2;//��ʾ����
	public static int NL_POINT=3;//��ʾ��
	
	
		
	public UnkownWordSeg(TrieTreeDictionary unknownDict) {
		super();
		this.unknownDict = unknownDict;
	}



	/**
	 * �ڴֵַĽ����ʶ�����ĸ�����ֺ����ַ�������һ��Ĵʣ���ϳ�һ����
	 * @param firstSegResult ��һ���ֵַĽ��
	 * @return ������ɺ�Ľ����list�Ĵ�СӦ��С�ڵ��������list��С
	 */
	List<WordItem> segNumLetter(List<WordItem> firstSegResult){
		
		String tagStr="";
		for (WordItem wordItem : firstSegResult) {
			WordItem item=unknownDict.searchWord(wordItem.getWord());
			if(item!=null)
				tagStr+=(char)(item.getPos().getTag()+'A');
			else
				tagStr+='A';			
		}
		
		log.info(tagStr);
		
		return null;
	}
	
	public static void main(String[] args) {
		TrieTreeDictionary unknownDic=TxtDicFileLoader.loadDic("dic/nl.dct");
		UnkownWordSeg seg=new UnkownWordSeg(unknownDic);
		//seg.segNumLetter(firstSegResult)
	}
	
	
}
