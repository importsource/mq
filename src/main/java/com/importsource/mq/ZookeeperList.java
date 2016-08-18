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

	public ZookeeperList(String config, String root) {
		this.root = root;
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
		try {
			return zk.getChildren(root, this).size();
		} catch (KeeperException e) {
			exception.add(e);
		} catch (InterruptedException e) {
			exception.add(e);
		}
		return 0;
	}

	public boolean isEmpty() {
		try {
			return zk.getChildren(root, this).size() == 0;
		} catch (KeeperException e) {
			exception.add(e);
		} catch (InterruptedException e) {
			exception.add(e);
		}
		return true;
	}

	public boolean contains(Object o) {
		try {
			Stat stat = zk.exists(root + o.toString(), this);
			return stat != null;
		} catch (KeeperException e) {
			exception.add(e);
		} catch (InterruptedException e) {
			exception.add(e);
		}
		return false;
	}

	public Iterator<E> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	public Object[] toArray() {
		try {
			List<String> children=zk.getChildren(root, this);
			if(children!=null){
				return children.toArray();
			}
		} catch (KeeperException e) {
			exception.add(e);
		} catch (InterruptedException e) {
			exception.add(e);
		}
		return null;
	}

	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean remove(Object o) {
		try {
			zk.delete(root + o.toString(), -1);
			return true;
		} catch (InterruptedException e) {
			exception.add(e);
		} catch (KeeperException e) {
			exception.add(e);
		}
		return false;
	}

	public boolean containsAll(Collection<?> c) {
		try {
			for (Iterator<?> iterator = c.iterator(); iterator.hasNext();) {
				Object o = (Object) iterator.next();
				boolean contains=contains(o);
				if(!contains){
					return false;
				}

			}
			return true;
		} catch (Exception e) {
			exception.add(e);
		}
		return false;
	}

	public boolean addAll(Collection<? extends E> c) {
		try {
			for (Iterator<?> iterator = c.iterator(); iterator.hasNext();) {
				E o = (E) iterator.next();
				this.add(o);
			}
			return true;
		} catch (Exception e) {
			exception.add(e);
		}
		return false;
	}

	public boolean removeAll(Collection<?> c) {
		try {
			 Iterator<?> listIterator = c.iterator();
			 while(listIterator.hasNext()){
				Object obj= listIterator.next();
				remove(obj);
			 }
			 return true;
		} catch (Exception e) {
			exception.add(e);
		}
		return false;
	}

	public boolean retainAll(Collection<?> c) {
		List<String> children;
		try {
			children = zk.getChildren(root, this);
			for (int i = 0; i < children.size(); i++) {
				String child=children.get(i);
				if(!c.contains(child)){
					remove(child);
					i--;
				}
			}
			return true;
		} catch (KeeperException e) {
			exception.add(e);
		} catch (InterruptedException e) {
			exception.add(e);
		}
		
		return false;
	}

	public void clear() {
		List<String> children = new ArrayList<String>();
		try {
			children = zk.getChildren(root, this);
		} catch (KeeperException e) {
			exception.add(e);
		} catch (InterruptedException e) {
			exception.add(e);
		}
		for (int i = 0; i < children.size(); i++) {
			remove(children.get(i));
			i--;
		}
	}

	public boolean add(E e) {
		return offer(e);
	}

	public boolean offer(E e) {
		try {
			return offer1(e.toString().getBytes());
		} catch (KeeperException e1) {
			exception.add(e1);
		} catch (InterruptedException e1) {
			exception.add(e1);
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
