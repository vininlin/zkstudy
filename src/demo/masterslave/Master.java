/**
 * 
 */
package demo.masterslave;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
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
    private volatile MasterStates state = MasterStates.RUNNING;
    
    private Random random = new Random(this.hashCode());
    String serverId = Integer.toHexString(random.nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    
    protected ChildrenCache workersCache;
    protected ChildrenCache tasksCache;
        
    Master(String hostPort){
        this.hostPort = hostPort;
    }
    
    void startZk() throws IOException{
        zk = new ZooKeeper(hostPort,15000,this);
    }
    
    void stopZk() throws InterruptedException{
        zk.close();
    }
    
    @Override
    public void process(WatchedEvent event) {
        LOG.info("Processing event " + event.toString() + "," + hostPort);
        if(event.getType() == EventType.None){
            switch(event.getState()){
            case SyncConnected:
                connected = true;
                break;
            case Disconnected:
                connected = false;
                break;
            case Expired:
                expired = true;
                connected = false;
                LOG.error("Session expiration");
                break;
            default:
                break;
            }
        }
    }
    
    boolean isConnected(){
        return connected;
    }
    
    boolean isExpired(){
        return expired;
    }
    
    void runForMaster() {
        LOG.info("Running for master");
        //改为异步
        zk.create("/master", serverId.getBytes(), 
                ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                CreateMode.EPHEMERAL,
                masterCreateCallback, null);
    }
    
    StringCallback masterCreateCallback = new StringCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            System.out.println("process master create result...");
            switch(Code.get(rc)){
                case CONNECTIONLOSS : 
                    checkMaster();
                    return;
                case OK:
                    state = MasterStates.ELECTED;
                    takeLeaderShip();
                    break;
                case NODEEXISTS:
                    state = MasterStates.NOTELECTED;
                    masterExists();
                    break;
                default :
                    state = MasterStates.NOTELECTED;
                    LOG.error("Something went wrong when running for master:" + KeeperException.create(Code.get(rc),path));
            }
            LOG.info("I'm " + (state == MasterStates.ELECTED ? "" : "not") + " the leader " + serverId);
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
    
    DataCallback masterCheckCallback = new DataCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                checkMaster();
                break;
            case NONODE:
                runForMaster();
                break;
            case OK:
                if(serverId.equals(new String(data))){
                    state = MasterStates.ELECTED;
                    takeLeaderShip();
                }else{
                    state = MasterStates.NOTELECTED;
                    masterExists();
                }
                break;
            default:
                LOG.error("Error when reading data.", 
                        KeeperException.create(Code.get(rc), path));
            }
        }
        
    };
    
    void masterExists(){
        zk.exists("/master", masterExistsWatcher, masterExsitsCallback, null);
    }
    
    Watcher masterExistsWatcher = new Watcher(){

        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == EventType.NodeDeleted){
                assert "/master".equals(event.getPath());
                runForMaster();
            }
        }
        
    };
    
    StatCallback masterExsitsCallback = new StatCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                masterExists();
                break;
            case OK:
                break;
            case NONODE:
                state = MasterStates.RUNNING;
                runForMaster();
                LOG.info("It sounds like the previous master is gone, " +
                        "so let's run for master again.");
                break;
             default:
                 checkMaster();
                 break;
            }
        }
        
    };
    
    void takeLeaderShip(){
        LOG.info("Going for list of workers");
        new RecoveredAssignments(zk).recover(new RecoverCallback(){
            
        });
    }
    
    /*
     ****************************************************
     **************************************************** 
     * Methods to handle changes to the list of workers.*
     ****************************************************
     ****************************************************
     */
    
    public int getWorkersSize(){
        return workersCache == null ? 0 : workersCache.getList().size();
    }
    
    void getWorkers(){
        zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
    }
    
    Watcher workersChangeWatcher = new Watcher(){

        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == EventType.NodeChildrenChanged){
                assert "/workers".equals(event.getPath());
                getWorkers();
            }
        }
        
    };
    
    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                getWorkers();
                break;
            case OK:
                LOG.info("Success got a list of workers:" + children.size() + " workers.");
                reassignAndSet(children);
                break;
            default:
                LOG.error("get Children failed:" + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    /*
     *******************
     *******************
     * Assigning tasks.*
     *******************
     *******************
     */
    
    void reassignAndSet(List<String> children){
        List<String> toProcess;
        if(workersCache == null){
            workersCache = new ChildrenCache();
            toProcess = null;
        }else{
            LOG.info("removing and setting:");
            toProcess = workersCache.removeAndSet(children);
        }
        
        if(toProcess != null){
            for(String worker : toProcess){
                getAbsentWorkerTasks(worker);
            }
        }
    }
    
    void getAbsentWorkerTasks(String worker){
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }
     
    ChildrenCallback workerAssignmentCallback = new ChildrenCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                getAbsentWorkerTasks(path);
                break;
            case OK:
                LOG.info("Success got a list of assignments:" + children.size() + " tasks.");
                for(String task : children){
                    getDataReassign(path,task);
                }     
                break;
            default:
                LOG.error("get Children failed:" + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. * 
     ************************************************
     */
    
    void getDataReassign(String path,String task){
        zk.getData(path, false, getDataReassignCallback, task);
    }
   
    DataCallback getDataReassignCallback = new DataCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                getDataReassign(path,(String)ctx);
                break;
            case OK:
                recreateTask(new RecreateTaskCtx(path,(String) ctx,data));    
                break;
            default:
                LOG.error("Something went wrong when getting data:" + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    class RecreateTaskCtx{
        String path;
        String task;
        byte[] data;
        
        RecreateTaskCtx(String path,String task,byte[] data){
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }
    
   void recreateTask(RecreateTaskCtx ctx){
       zk.create("/tasks/" + ctx.task, 
               ctx.data, 
               Ids.OPEN_ACL_UNSAFE, 
               CreateMode.PERSISTENT,
               recreateTasksCallback,
               ctx);
   }
   
   StringCallback recreateTasksCallback = new StringCallback(){

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        switch(Code.get(rc)){
        case CONNECTIONLOSS : 
            recreateTask((RecreateTaskCtx)ctx);
            break;
        case OK:
            deleteAssignment(path);    
            break;
        case NODEEXISTS:
            LOG.info("Node exists already, but if it hasn't been deleted, " +
                    "then it will eventually, so we keep trying: " + path);
            recreateTask((RecreateTaskCtx)ctx);
            break;
        default:
            LOG.error("Something went wrong when recreating task",KeeperException.create(Code.get(rc)));
        }
    }
       
   };
   
   void deleteAssignment(String path){
       zk.delete(path, -1, taskDelectionCallback, null);
   }
   
   VoidCallback taskDelectionCallback = new VoidCallback(){

    @Override
    public void processResult(int rc, String path, Object ctx) {
        switch(Code.get(rc)){
        case CONNECTIONLOSS : 
            deleteAssignment(path);
            break;
        case OK:
            LOG.info("Task correctly deleted: " + path);
            break;
        default:
            LOG.error("Failed to delete task data:",KeeperException.create(Code.get(rc)));
        }
    }
       
   };
   
   /*
    ******************************************************
    ******************************************************
    * Methods for receiving new tasks and assigning them.*
    ******************************************************
    ******************************************************
    */
   
    
    public static void main(String[] args) throws Exception{
        String hostPort = "localhost:2181,localhost:2182,localhost:2183";
        Master master = new Master(hostPort);
        master.startZk();
        master.runForMaster();
        master.bootstrap();
        //异步不需要以下
       /* if(isLeader){
            LOG.info("I'm the leader");
            Thread.sleep(6000);
        }else{
            LOG.info("Someone else is the leader");
        }*/
        Thread.sleep(6000);
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
