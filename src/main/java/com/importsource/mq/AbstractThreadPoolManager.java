package com.importsource.mq;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import com.importsource.log.client.LogManager;
import com.importsource.log.core.Logger;

/**
 * 
 * @author Hezf
 *
 */
public abstract class AbstractThreadPoolManager {
	Logger logger = LogManager.getLogger(AbstractThreadPoolManager.class);
	private final static int CORE_POOL_SIZE = 4;// 线程池维护线程的最少数量
	private final static int MAX_POOL_SIZE = 50;// 线程池维护线程的最大数量
	private final static int KEEP_ALIVE_TIME = 0;// 线程池维护线程所允许的空闲时间
	private final static int WORK_QUEUE_SIZE = 2000;// 线程池所使用的缓冲队列大小
	protected static Queue<String> msgQueue =new LinkedList<String>();// 消息缓冲队列
	// 访问消息缓存的调度线程
	protected static String clz;

	protected final Runnable accessBufferThread = new Runnable() {
		public void run() {
			// 查看是否有待定请求，如果有，则创建一个新的AccessDBThread，并添加到线程池中
		    doIt();
		}
	};

	protected final RejectedExecutionHandler handler = new RejectedExecutionHandler() {

		public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
			logger.i(((AbstractThread) r).getMsg() + "消息放入队列中重新等待执行");
			msgQueue.offer(((AbstractThread) r).getMsg());
		}
	};
	// 管理线程池
	protected final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE_TIME,
			TimeUnit.SECONDS, new ArrayBlockingQueue(WORK_QUEUE_SIZE), this.handler);
	// 调度线程池
	protected final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	protected final ScheduledFuture taskHandler = scheduler.scheduleAtFixedRate(accessBufferThread, 0, 1, TimeUnit.NANOSECONDS);
	protected abstract void doIt() ;
	
	protected boolean hasMoreAcquire() {
		return !msgQueue.isEmpty();
	}


}