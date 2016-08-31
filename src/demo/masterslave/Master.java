/**
 * 
 */
package demo.masterslave;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 类/接口注释
 * 
 * @author linwn@ucweb.com
 * @createDate 2016-8-30
 * 
 */
public class Master implements Watcher {
    
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    ZooKeeper zk;
    String hostPort;
    
    Master(String hostPort){
        this.hostPort = hostPort;
    }
    
    void startZk() throws IOException{
        zk = new ZooKeeper(hostPort,15000,this);
    }
    
    void stopZk() throws InterruptedException{
        zk.close();
    }
    
    String serverId = Integer.toHexString(new Random().nextInt());
    static boolean isLeader = false;
    StringCallback masterCreateCallback = new StringCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)){
                case CONNECTIONLOSS : 
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default :
                    isLeader = false;
            }
            LOG.info("I'm the " + (isLeader ? "" : "not") + "leader");
        }
        
    };
    
    DataCallback masterCheckCallback = new DataCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                checkMaster();
                return;
            case NONODE:
                runForMaster();
                return;
            }
        }
        
    };
            
    void checkMaster(){
        /*while(true){
                Stat stat = new Stat();
                byte[] data;
                try {
                    data = zk.getData("/master", false, stat);
                    isLeader = serverId.equals(new String(data));
                    return true;
                } catch (KeeperException e) {
                    e.printStackTrace();
                    return false;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }*/
        //改为异步
        zk.getData("/master", false, masterCheckCallback, null);
    }
    
    void runForMaster() {
        /*while(true){
            try {
                zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
            } catch (KeeperException e) {
                e.printStackTrace();
                isLeader = false;
                break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(checkMaster()){
                break;
            }
        }*/
        //改为异步
        zk.create("/master", serverId.getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL,
                masterCreateCallback, null);
    }
    
    
    @Override
    public void process(WatchedEvent event) {
        LOG.info(event.toString() + "," + hostPort);
    }
    
    public static void main(String[] args) throws Exception{
        String hostPort = "localhost:2181,localhost:2182,localhost:2183";
        Master master = new Master(hostPort);
        master.startZk();
        master.runForMaster();
        if(isLeader){
            LOG.info("I'm the leader");
            Thread.sleep(6000);
        }else{
            LOG.info("Someone else is the leader");
        }
        
        master.stopZk();
    }
    
    void bootstrap(){
        createParent("/workers",new byte[0]);
        createParent("/assign",new byte[0]);
        createParent("/tasks",new byte[0]);
        createParent("/status",new byte[0]);
    }
    
    void createParent(String path,byte[] data){
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT, parentCreateCallback, data);
    }
    
    StringCallback parentCreateCallback = new StringCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)){
                case CONNECTIONLOSS : 
                    createParent(path,(byte[])ctx);
                    break;
                case OK:
                    LOG.info("Parent create.");
                    break;
                case NODEEXISTS:
                    LOG.warn("Parent aready registerd:" +  path);
                    break;
                default :
                    LOG.error("Something went wrong:" + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };

}
