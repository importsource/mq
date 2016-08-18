package com.importsource.mq.demo;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.importsource.mq.DefaultThreadPoolManager;
import com.importsource.mq.Producer;
import com.importsource.mq.ZookeeperList;


/**
 * 消息输送前端。客户端请求发送的api实现
 * @author Hezf
 *
 */
public class Demo {
	public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
		//线程池管理者。然后加入消息
		Producer tpm = DefaultThreadPoolManager.newInstance("com.importsource.mq.DBAccessThread",new ZookeeperList("127.0.0.1:2181","/default2"));
		long start=System.currentTimeMillis();
		for (int i = 0; i < 2051; i++) {
			tpm.addMsg(String.valueOf(i));
		}
		long end=System.currentTimeMillis();
		System.out.println(end-start);
		
		
		
	}
}