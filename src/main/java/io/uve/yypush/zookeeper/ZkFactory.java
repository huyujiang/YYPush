package io.uve.yypush.zookeeper;

/**
 * 
 * @author yangyang21@staff.weibo.com(yangyang)
 * 
 */
public class ZkFactory {
	private static volatile boolean expire = false;
	private static volatile ZkConfig zkConfig = null;
	private final static String connectStr;

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
					ZkConfig tmp = new ZkConfig(connectStr);
					if (expire) {
						tmp.handleExpare();
						expire = false;
					}
					zkConfig = tmp;
				}
			}
		}
		return zkConfig;
	}

	public static void expire() {
		synchronized (ZkFactory.class) {
			expire = true;
			zkConfig = null;
		}
	}
}
