package org.loonglab.segment.nshort;

import java.util.ArrayList;
import java.util.List;

import org.loonglab.segment.dictionary.WordItem;


public class SentenceSeg {
	
	// Seperator type
	public static final String SEPERATOR_C_SENTENCE = "������������";

	public static final String SEPERATOR_C_SUB_SENTENCE = "����������������";

	public static final String SEPERATOR_E_SENTENCE = "!?:;";

	public static final String SEPERATOR_E_SUB_SENTENCE = ",()\"'";

	//public static final String SEPERATOR_LINK = "\n\r ��";
	public static final String SEPERATOR_LINK = "\n\r";
	
	private String src;
	private List<Sentence> sens;
	
	public SentenceSeg(String src){
		this.src=src;
		sens=split();
	}
	/**
	 * ���о��ӷָ�
	 * 
	 * @param src
	 * @return
	 */
	private List<Sentence> split( ) {
		List<Sentence> result = null;

		if (src != null) {
			result = new ArrayList<Sentence>();
			String s1 = WordItem.SENTENCE_BEGIN;

			for (int i = 0; i < src.length(); i++) {
				char ss=src.charAt(i);
				// ����Ƿָ���������س�����/���ŵ�
				if (SEPERATOR_C_SENTENCE.indexOf(ss) != -1
						|| SEPERATOR_LINK.indexOf(ss) != -1) {
					
					if(SEPERATOR_C_SENTENCE.indexOf(ss) != -1){
						//����Ǿ�ĩ�ָ��������з����Ե�
						s1 += ss;
						s1 += WordItem.SENTENCE_END;
						result.add(new Sentence(s1, true));
						s1 = WordItem.SENTENCE_BEGIN;
					}
				} 
				else
					s1 += ss;
			}

			if (s1.length() > 0 && !WordItem.SENTENCE_BEGIN.equals(s1)) {
				s1 += WordItem.SENTENCE_END;
				result.add(new Sentence(s1, true));
			}
		}
		return result;
	}
	public List<Sentence> getSens() {
		return sens;
	}
	
	
}
