package com.importsource.mq.util;

import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;

/**
 * @author Hezf
 *
 */
public class ZKOperationHelper {
	public static void del() {
		ZooKeeper zooKeeper = null;
		try {
			zooKeeper = new ZooKeeper("127.0.0.1:2181", 3000, null);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		for (int j = 264; j < 100000; j++) {
			int zeroLength = 10 - ((j + "").length());
			try {
				StringBuilder zeroSeq = new StringBuilder();
				for (int i = 0; i < zeroLength; i++) {
					zeroSeq.append("0");
				}
				zooKeeper.delete("/defaultQueue/element" + zeroSeq.toString() + j, -1);
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
	}
}
