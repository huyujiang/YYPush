package io.uve.yypush.zookeeper;

import org.apache.log4j.Logger;

/**
 * 
 * @author yangyang21@staff.weibo.com(yangyang)
 * 
 */
public class ZkFactory {
	private static volatile ZkConfig zkConfig = null;
	private final static String connectStr;
	private static Logger log = Logger.getLogger(ZkFactory.class);

	static {
		String zookeeper = System.getenv("zookeeper");
		if (zookeeper == null) {
			connectStr = "172.16.89.130:2181,172.16.89.128:2181,172.16.89.129:2181";
		} else {
			connectStr = zookeeper;
		}
	}

	public static ZkConfig getZkConfig() {
		if (zkConfig == null) {
			synchronized (ZkFactory.class) {
				if (zkConfig == null) {
					zkConfig = new ZkConfig(connectStr);
				}
			}
		}
		return zkConfig;
	}

	public static void resetZkConfig() {
		boolean renew = true;
		ZkConfig zk = null;
		while(renew) {
			zk = new ZkConfig(connectStr);
			boolean retry = true;
			while (retry) {
				try {
					/**
					 * if return false, I will re new Zkconfig
					 */
					renew = !zk.handleExpare();
					if(renew){
						log.info("zookeeper handle expare has catch keeper exceptin and re new zkconfig!");
						break;
					}
					retry = false;
				} catch (InterruptedException e) {
					log.info("zookeeper handle expare has been inter and retry!");
				}
			}
		}

		log.info("zookeeper handle expare success!");
		zkConfig = zk;
	}
}
