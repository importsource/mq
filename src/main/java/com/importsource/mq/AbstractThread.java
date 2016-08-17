package com.importsource.mq;

/**
 * 实现一个通用抽象线程
 * @author Hezf
 *
 */
public abstract class AbstractThread implements Runnable {
	protected String msg;

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public AbstractThread() {
		super();
	}

	public AbstractThread(String msg) {
		this.msg = msg;
	}

	public void run() {
		// 向数据库中添加Msg变量值
		//System.out.println("Added the message: " + msg + " into the Database");
		doIt();
	}

	public abstract void doIt() ;
}
