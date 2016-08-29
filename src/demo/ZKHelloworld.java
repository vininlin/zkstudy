/**
 * 
 */
package demo;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Àà/½Ó¿Ú×¢ÊÍ
 * 
 * @author linwn@ucweb.com
 * @createDate 2016-8-29
 * 
 */
public class ZKHelloworld {

    /**
     * @param args
     */
    public static void main(String[] args) {
       try {
        ZooKeeper zk = new ZooKeeper("localhost:2181,localhost:2182,localhost:2183,",30000,new HelloWatcher());
        String node = "/app1";
        Stat stat = zk.exists(node, false);
        if(stat == null){
            String createResult = zk.create(node, "test".getBytes(), 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(createResult);
        }
        byte[] b = zk.getData(node, false, stat);
        System.out.println(new String(b));
        zk.close();
    } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }

    }
    
    static class HelloWatcher implements Watcher{

        @Override
        public void process(WatchedEvent event) {
            System.out.println("path:" + event.getPath());
            System.out.println("type:" + event.getType());
            System.out.println("state:" + event.getState());
        }
        
    }

}
