package io.uve.yypush.zookeeper;

import org.apache.log4j.Logger;

/**
 * 
 * @author yangyang21@staff.weibo.com(yangyang)
 * 
 */
public enum ZkMonitorPath {
	instance;
	private static Logger log = Logger.getLogger(ZkMonitorPath.class);

	public boolean register(String name) {
		try {
			boolean succ = false;
			while (!succ) {
				succ = ZkFactory.getZkConfig().register(name);
				if(!succ){
					log.error("cancel" + name + "keeper Execption: will retry 2s later!");
					Thread.sleep(2000L);
				}
			}
			return true;
		} catch (InterruptedException e) {
			log.info("may recv interrupt and regisetr fail");
		}
		return false;
	}

	public boolean cancel(String name) {
		try {
			boolean succ = false;
			while (!succ) {
				succ = ZkFactory.getZkConfig().cancel(name);
				if(!succ){
					log.error("canel" + name + "keeper Execption: will retry 2s later!");
					Thread.sleep(2000L);
				}
			}
			return true;
		} catch (InterruptedException e) {
			log.info("may recv interrupt and regisetr fail");
		}
		return false;
	}
	public void heart(String name) {
		ZkFactory.getZkConfig().heart(name);
	}
}
