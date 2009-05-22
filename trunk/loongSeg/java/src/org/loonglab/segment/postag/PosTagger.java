package org.loonglab.segment.postag;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ictclas4j.bean.SegNode;
import org.ictclas4j.utility.Utility;
import org.loonglab.segment.SegmentException;
import org.loonglab.segment.dictionary.POS;
import org.loonglab.segment.dictionary.WordItem;
import org.loonglab.segment.dictionary.trie.TrieTreeDictionary;

public class PosTagger {

	static Log log = LogFactory.getLog(PosTagger.class);

	protected TrieTreeDictionary coreDict;
	protected TrieTreeDictionary unknownDict;
	protected ContextStat context;
	

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
	public List<TagNode> posUnkownTag(List<WordItem> firstSegResult){
		
		//��Ҫ����TagNode�࣬����м���������viterbi�㷨���Ա�ע
		List<TagNode> tagNodes=new ArrayList<TagNode>();
		
		//��1����ÿ���ʽ��б�ע
		for (WordItem wordItem : firstSegResult) {
			WordItem tagItem=unknownDict.searchWord(wordItem.getWord());

			TagNode tagNode=new TagNode();
			tagNode.setWordItem(wordItem);
			
			//��δ��¼�ʵ����Ĵ���
			if(tagItem!=null){				
				List<POS> posList=tagItem.getPosList();
				for (POS pos : posList) {
					PosLink link=new PosLink();
					link.setPos(pos);
					double freq = -Math.log((1 + pos.getFreq()));
					freq += Math.log((context.getFreq(0, pos.getTag()) + posList.size() + 1));
					link.setValue(freq);
					
					tagNode.getLinks().add(link);
				}
			}

			//δ��¼��ʶ��ʱ����ͨ���Զ���עΪ0
			if(!wordItem.getWord().equals(WordItem.SENTENCE_BEGIN)&&!wordItem.getWord().equals(WordItem.SENTENCE_END)){
				double freq = -Math.log((double) (1 + wordItem.getFreq()));
				freq += Math.log((double) (context.getFreq(0, 0) + wordItem.getPosList().size()));
				PosLink link=new PosLink();
				link.setPos(new POS(0,wordItem.getFreq()));
				link.setValue(freq);			
				tagNode.getLinks().add(link);
			}
						
			
			tagNodes.add(tagNode);	
		}
		
		
		//��2��vertibi�㷨�õ���Ѵ�������
		getOptTag(tagNodes);

		
		return tagNodes;
		

		
	}

	private void getOptTag(List<TagNode> tagNodes) {
		List<PosLink> prevAllPosLink = null;
		List<PosLink> allPosLink = null;
		for (int i = 1; i < tagNodes.size(); i++) {
			prevAllPosLink = tagNodes.get(i - 1).getLinks();

			allPosLink = tagNodes.get(i).getLinks(); 

			for (PosLink posLink : allPosLink) {
				int minPrev = 0;
				double minFreq = 1000;
				for (int k = 0;prevAllPosLink!=null &&  k < prevAllPosLink.size(); k++) {
					PosLink prevPosLink = prevAllPosLink.get(k);
					double temp = context.getPossibility(0, prevPosLink.getPos().getTag(), posLink.getPos().getTag());

					temp = -Math.log(temp) + prevPosLink.getValue();
					if (temp < minFreq) {
						minFreq = temp;
						minPrev = k;
					}
				}

				posLink.setPrev(minPrev);
				posLink.setValue((posLink.getValue() + minFreq));
			}
		}


		for (int i = tagNodes.size() - 1, j = 0; i >= 0; i--) {
			List<PosLink> links = tagNodes.get(i).getLinks();
			PosLink posLink = links.get(j);
			posLink.getPos().setBest(true);
			j = posLink.getPrev();
		}
	}
	
	public List<WordItem> posTag(List<WordItem> optSegResult){
		//��Ҫ����TagNode�࣬����м���������viterbi�㷨���Ա�ע
		List<TagNode> tagNodes=new ArrayList<TagNode>();
		
		//��1����ÿ���ʽ��б�ע
		for (WordItem wordItem : optSegResult) {

			TagNode tagNode=new TagNode();
			tagNode.setWordItem(wordItem);
			
			List<POS> posList=wordItem.getPosList();
			for (POS pos : posList) {
				PosLink link=new PosLink();
				link.setPos(pos);
				double freq = -Math.log((1 + pos.getFreq()));
				freq += Math.log((context.getFreq(0, pos.getTag()) + posList.size() + 1));
				link.setValue(freq);
				
				tagNode.getLinks().add(link);
			}
						
			
			tagNodes.add(tagNode);	
		}
		
		getOptTag(tagNodes);
		
		List<WordItem> results=new ArrayList<WordItem>();
		for (TagNode tagNode : tagNodes) {

			WordItem item=new WordItem(tagNode.getWordItem().getWord());
			item.setSrcWord(tagNode.getWordItem().getSrcWord());
			for (PosLink link : tagNode.getLinks()) {
				POS pos=new POS(link.getPos().getTag(),link.getPos().getFreq());
				pos.setBest(link.getPos().isBest());
				item.addPos(pos);
			}
			results.add(item);

		}
		
		return results;
	}
	
	
	

	
	

}
