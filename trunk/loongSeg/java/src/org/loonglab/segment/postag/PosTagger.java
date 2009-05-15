package org.loonglab.segment.postag;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.loonglab.segment.dictionary.WordItem;
import org.loonglab.segment.dictionary.trie.TrieTreeDictionary;

public class PosTagger {

	static Log log = LogFactory.getLog(PosTagger.class);

	protected TrieTreeDictionary coreDict;
	protected TrieTreeDictionary unknownDict;
	protected ContextStat context;
	
//	public static final String SENTENCE_BEGIN = "ʼ##ʼ";
//
//	public static final String SENTENCE_END = "ĩ##ĩ";

	public PosTagger(TrieTreeDictionary coreDic, TrieTreeDictionary unknownDic,
			ContextStat context) {
		this.coreDict = coreDic;
		this.unknownDict = unknownDic;
		this.context = context;
	}

	/**
	 * ��N���·���ַֽ�����б�ע������ʶ��δ��¼��
	 * 
	 * @param firstSegResult
	 * @return
	 */
	public void posUnkownTag(List<WordItem> firstSegResult){
		
		//��Ҫ����TagNode�࣬����м���������viterbi�㷨���Ա�ע
		List<WordItem> tagResult=new ArrayList<WordItem>();
		
		//��ÿ���ʽ��б�ע
		for (WordItem wordItem : firstSegResult) {
			WordItem tagItem=unknownDict.searchWord(wordItem.getWord());

			if(tagItem!=null){
				tagItem.addPos(0, wordItem.getFreq());
			}
			else{
				tagItem=new WordItem(wordItem.getWord(),0,wordItem.getFreq());
			}

			tagResult.add(tagItem);	
		}
		
		//vertibi�㷨�õ���������
		
	}

}
