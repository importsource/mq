package com.importsource.mq;

import java.util.Queue;

import com.importsource.log.client.LogManager;
import com.importsource.log.core.Logger;

/**
 * 
 * @author Hezf
 *
 */
public class DefaultThreadPoolManager extends AbstractThreadPoolManager implements Producer {
	Logger logger = LogManager.getLogger(DefaultThreadPoolManager.class);
	
	private static DefaultThreadPoolManager tpm = new DefaultThreadPoolManager();

	
	// 访问消息缓存的调度线程
	private DefaultThreadPoolManager() {
	}

	/**
	 * 实例化一个线程池管理器
	 * 
	 * @param clz
	 *            传入消费者处理逻辑类
	 * @return DefaultThreadPoolManager
	 */
	public static DefaultThreadPoolManager newInstance(String clz) {
		DefaultThreadPoolManager.clz = clz;
		return tpm;
	}

	/**
	 * 实例化一个线程池管理器
	 * 
	 * @param clz
	 *            传入消费者处理逻辑类
	 * @param msgQueue
	 *            传入存储消息的队列实现
	 * @return DefaultThreadPoolManager
	 */
	public static DefaultThreadPoolManager newInstance(String clz, Queue<String> msgQueue) {
		DefaultThreadPoolManager.clz = clz;
		DefaultThreadPoolManager.msgQueue = msgQueue;
		return tpm;
	}

	public void addMsg(String msg) {
		AbstractThread task = null;
		try {
			task = (AbstractThread) Class.forName(clz).newInstance();
		} catch (InstantiationException e) {
			logger.e(e.getMessage());
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			logger.e(e.getMessage());
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			logger.e(e.getMessage());
			throw new RuntimeException(e);
		}
		task.setMsg(msg);
		threadPool.execute(task);
	}

	@Override
	protected void doIt() {
		
		// 查看是否有待定请求，如果有，则创建一个新的AccessDBThread，并添加到线程池中
		if (hasMoreAcquire()) {
			String msg = (String) msgQueue.poll();
			addMsg(msg);
		}
	}

}