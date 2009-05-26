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

	public static final String SEPERATOR_LINK = "\n\r ��";
	
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
						|| SEPERATOR_LINK.indexOf(ss) != -1
						|| SEPERATOR_C_SUB_SENTENCE.indexOf(ss) != -1
						|| SEPERATOR_E_SUB_SENTENCE.indexOf(ss) != -1) {
					// ������ǻس����кͿո�
					if (SEPERATOR_LINK.indexOf(ss) == -1)
						s1 += ss;
					// �Ͼ�
					if (s1.length() > 0 && !WordItem.SENTENCE_BEGIN.equals(s1)) {
						if (SEPERATOR_C_SUB_SENTENCE.indexOf(ss) == -1
								&& SEPERATOR_E_SUB_SENTENCE
										.indexOf(ss) == -1)
							s1 += WordItem.SENTENCE_END;

						result.add(new Sentence(s1, true));
						s1 = "";
					}

					// �ǻس����з���ո�����Ҫ���з�������
					if (SEPERATOR_LINK.indexOf(ss) != -1) {
						result.add(new Sentence(ss+""));
						s1 = WordItem.SENTENCE_BEGIN;

					} else if (SEPERATOR_C_SENTENCE.indexOf(ss) != -1
							|| SEPERATOR_E_SENTENCE.indexOf(ss) != -1)
						s1 = WordItem.SENTENCE_BEGIN;
					else s1 = WordItem.SENTENCE_BEGIN;
//						s1 = ss;

				} else
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
