package org.loonglab.segment.nshort;

import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SentenceSegTest extends TestCase {

	static Log log = LogFactory.getLog(SentenceSegTest.class);
	
	public void testSentenceSeg() {
		SentenceSeg ss=new SentenceSeg("����ѡ�Ĳ����㻨֦��չ��ʱ��Ҳ��������˴�Լʱ�򣬶��ǿ����ѵ������յ��ա������泯������ӡ�");
		List<Sentence> sList=ss.getSens();
		
		for (Sentence sentence : sList) {
			log.info(sentence);
		}
	}

}
