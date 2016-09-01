/**
 * 
 */
package demo.masterslave;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Àà/½Ó¿Ú×¢ÊÍ
 * 
 * @author linwn@ucweb.com
 * @createDate 2016-9-1
 * 
 */
public class Client implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    ZooKeeper zk;
    String hostPort;
    
    public Client(String hostPort){
        this.hostPort = hostPort;
    }
  
    @Override
    public void process(WatchedEvent event) {
        LOG.info(event.toString() + "," + hostPort);
    }
    
    void startZk() throws IOException{
        zk = new ZooKeeper(hostPort,15000,this);
    }
    
    String queueCommand(String command){
        while(true){
            try {
                String name = zk.create("/tasks/task-", command.getBytes(),
                        Ids.OPEN_ACL_UNSAFE, 
                        CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) throws Exception{
        String hostPort = "localhost:2181,localhost:2182,localhost:2183";
        Client c = new Client(hostPort);
        c.startZk();
        String command = "go";
        String name = c.queueCommand(command);
        LOG.info("Created " +name);
    }

}
