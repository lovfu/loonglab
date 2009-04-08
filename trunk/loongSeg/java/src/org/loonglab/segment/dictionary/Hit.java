package org.loonglab.segment.dictionary;

/**
 * ���ҵĽ��
 * @author loonglab
 *
 */
public class Hit {
	
	/**
	 * ��ѯ�Ĵʴ�
	 */
	String searchWord;
	
	/**
	 * ���ҵ��ĵ��ʣ���û�в��ҵ�����Ϊnull
	 */
	WordItem word;
	
	/**
	 * ��ʾ��ѯ�����״̬
	 */
	int state;
	
	/**
	 * û���ҵ�
	 */
	public static int STATE_NONE=-1;
	/**
	 * û�ҵ���������ǰ���ַ���ͬ�ĵ���
	 */
	public static int STATE_NEXT=0;
	/**
	 * �ҵ��õ���
	 */
	public static int STATE_HIT=1;
	
	
	
	
	public Hit() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Hit(String searchWord, WordItem word, int state) {
		super();
		this.searchWord = searchWord;
		this.word = word;
		this.state = state;
	}

	public String getSearchWord() {
		return searchWord;
	}

	public void setSearchWord(String searchWord) {
		this.searchWord = searchWord;
	}

	public WordItem getWord() {
		return word;
	}

	public void setWord(WordItem word) {
		this.word = word;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}
	
	
	
	
}
