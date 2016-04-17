package com.importsource.mq;


public class TestDriver {
	public static void main(String[] args) {
		ThreadPoolManager tpm = ThreadPoolManager.newInstance("com.importsource.mq.DBAccessThread");
		for (int i = 0; i < 50000; i++) {
			tpm.addMsg(String.valueOf(i));
		}

	}
}