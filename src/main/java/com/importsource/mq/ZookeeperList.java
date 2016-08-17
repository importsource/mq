package com.importsource.mq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 实现一个基于zookeeper的列表用来实现消息队列
 * 
 * @author Hezf
 *
 */
public class ZookeeperList<E> implements Queue<E>, Watcher {
	private ZooKeeper zk;
	private int sessionTimeout = 30000;
	private String root = "/defaultQueue";// 根
	private List<Exception> exception = new ArrayList<Exception>();
	private final Object mutex = new Object();

	public ZookeeperList(String config) {
		connect(config);
	}
	
	public ZookeeperList(String config,String root) {
		this.root=root;
		connect(config);
	}

	private void connect(String config) {
		// 创建一个与服务器的连接
		try {
			zk = new ZooKeeper(config, sessionTimeout, this);
			Stat stat = zk.exists(root, false);
			if (stat == null) {
				// 创建根节点
				zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (IOException e) {
			exception.add(e);
		} catch (KeeperException e) {
			exception.add(e);
		} catch (InterruptedException e) {
			exception.add(e);
		}
	}

	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isEmpty() {

		// TODO Auto-generated method stub
		return false;
	}

	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}

	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean addAll(Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	public void clear() {
		// TODO Auto-generated method stub

	}

	public boolean add(E e) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean offer(E e) {
		try {
			return offer1(e.toString().getBytes());
		} catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return false;
	}

	public E remove() {
		// TODO Auto-generated method stub
		return null;
	}

	public E poll() {
		try {
			return (E) poll1();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public E element() {
		// TODO Auto-generated method stub
		return null;
	}

	public E peek() {
		// TODO Auto-generated method stub
		return null;
	}

	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub

	}

	private boolean offer1(byte[] value) throws KeeperException, InterruptedException {
		zk.create(root + "/element", value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		return true;
	}

	private byte[] poll1() throws KeeperException, InterruptedException {

		int retvalue = -1;

		Stat stat = null;

		while (true) {

			synchronized (mutex) {

				List<String> list = zk.getChildren(root, true);

				if (list.size() == 0) {

					mutex.wait();

				} else {

					Integer min = new Integer(list.get(0).substring(7));

					for (String s : list) {

						Integer tempValue = new Integer(s.substring(7));

						if (tempValue < min)
							min = tempValue;

					}

					byte[] b = zk.getData(root + "/element" + min, false, stat);

					zk.delete(root + "/element" + min, 0);

					return b;

				}

			}

		}

	}

}
