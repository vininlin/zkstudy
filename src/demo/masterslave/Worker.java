/**
 * 
 */
package demo.masterslave;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Àà/½Ó¿Ú×¢ÊÍ
 * 
 * @author linwn@ucweb.com
 * @createDate 2016-8-31
 * 
 */
public class Worker implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    ZooKeeper zk;
    String hostPort;
    String serverId = Integer.toHexString(new Random().nextInt());
    String status;
    
    Worker(String hostPort){
        this.hostPort = hostPort;
    }
    
    void startZk() throws IOException{
        zk = new ZooKeeper(hostPort,15000,this);
    }
    
    @Override
    public void process(WatchedEvent event) {
        LOG.info(event.toString() + "," + hostPort);
    }
    
    void register(){
        zk.create("/workers/worker-" + serverId, 
                "Idle".getBytes(), 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null);
    }

    StringCallback createWorkerCallback = new StringCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                register();
                return;
            case OK:
                LOG.info("Registered success ,serverId:" + serverId);
                break;
            case NODEEXISTS:
                LOG.warn("Aready registered ,serverId:" + serverId);
                break;
            default :
                LOG.error("Something went wrong:" + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    StatCallback statusUpdateCallback = new StatCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                updateStatus((String)ctx);
                return;
            }
        }
        
    };
    
    private synchronized void updateStatus(String status){
        if(status == this.status){
            zk.setData("/workers/" + serverId, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }
    
    public void setStatus(String status){
        this.status = status;
        updateStatus(status);
    }
   
    
    void getWorkerList(){
        
    }
    
    ChildrenCache workersCache;
   
    
    public static void main(String[] args) throws Exception{
        String hostPort = "localhost:2181,localhost:2182,localhost:2183";
        Worker worker = new Worker(hostPort);
        worker.startZk();
        worker.register();
        Thread.sleep(30000);
    }
}
