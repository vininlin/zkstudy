/**
 * 
 */
package demo.masterslave;

import java.io.IOException;
import java.util.Date;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Àà/½Ó¿Ú×¢ÊÍ
 * 
 * @author linwn@ucweb.com
 * @createDate 2016-9-1
 * 
 */
public class AdminClient implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(AdminClient.class);
    ZooKeeper zk;
    String hostPort;
    
    public AdminClient(String hostPort){
        this.hostPort = hostPort;
    }
    
    void startZk() throws IOException{
        zk = new ZooKeeper(hostPort,15000,this);
    }
    
    @Override
    public void process(WatchedEvent event) {
        LOG.info(event.toString() + "," + hostPort);
    }
    
    void listState() throws KeeperException, InterruptedException{
        Stat stat = new Stat();
        byte[] masterData = zk.getData("/master", false, stat);
        Date startDate = new Date(stat.getCtime());
        LOG.info("Master:" + new String(masterData) + " since " + startDate);
        
        LOG.info("workers:");
        for(String w : zk.getChildren("/workers", false)){
            byte[] data = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            LOG.info("Worker:"+state);
        }
        LOG.info("tasks:");
        for(String t : zk.getChildren("/assign", false)){
            LOG.info("Task:" + t );
        }
    }

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        String hostPort = "localhost:2181,localhost:2182,localhost:2183";
        AdminClient a = new AdminClient(hostPort);
        a.startZk();
        a.listState();
    }

}
