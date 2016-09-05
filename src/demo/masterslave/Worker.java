/**
 * 
 */
package demo.masterslave;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
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
public class Worker implements Watcher,Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    ZooKeeper zk;
    String hostPort;
    private Random random = new Random(this.hashCode());
    String serverId = Integer.toHexString(random.nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;
    
    private ThreadPoolExecutor executor;
    
    Worker(String hostPort){
        this.hostPort = hostPort;
        this.executor = new ThreadPoolExecutor(1,1,
                1000L,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<Runnable>(200));
    }
    
    void startZk() throws IOException{
        zk = new ZooKeeper(hostPort,15000,this);
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
    
    public void bootstrap(){
        createAssignNode();
    }
    
    void createAssignNode(){
        zk.create("/assign/worker-" + serverId, new byte[0], 
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, 
                createAssignCallback, null);
    }
    
    StringCallback createAssignCallback = new StringCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                createAssignNode();
                break;
            case OK:
                LOG.info("Assign node created:" + serverId);
                break;
            case NODEEXISTS:
                LOG.warn("Assign node Aready create");
                break;
            default :
                LOG.error("Something went wrong:" + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    String name;
    
    void register(){
        name = "work-" + serverId; 
        zk.create("/workers/" + name, 
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
                break;
            case OK:
                LOG.info("Registered successfully ,serverId:" + serverId);
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
    
    String status;
    
    private synchronized void updateStatus(String status){
        if(status == this.status){
            zk.setData("/workers/" + name, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }
    
    public void setStatus(String status){
        this.status = status;
        updateStatus(status);
    }
   
    private int executionCount;
    
    synchronized void changeExecutionCount(int countChange){
        if(executionCount == 0 && countChange < 0){
            setStatus("Idle");
        }
        if(executionCount == 1 && countChange > 0){
            setStatus("Working"); 
        }
    }
   
    /*
     *************************************** 
     ***************************************
     * Methods to wait for new assignments.*
     *************************************** 
     ***************************************
     */
    
    Watcher newTaskWatcher = new Watcher(){

        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == EventType.NodeChildrenChanged){
                assert new String("/assign/worker" + serverId).equals(event.getPath());
                getTasks();
            }
        }
        
    };
    
    void getTasks(){
        zk.getChildren("/assign/worker" + serverId, newTaskWatcher, 
                taskGetChildrenCallback, null);
    }
    
    protected ChildrenCache assignedTaskCache = new ChildrenCache();
    
    ChildrenCallback taskGetChildrenCallback = new ChildrenCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                getTasks();
                break;
            case OK:
                if(children != null){
                    executor.execute(new Runnable(){

                        List<String> children;
                        DataCallback cb;
                        
                        public Runnable init(List<String> children, DataCallback cb){
                            this.children = children;
                            this.cb = cb;
                            return this;
                        }
                        
                        @Override
                        public void run() {
                            if(children == null){
                                return;
                            }
                            LOG.info("Looping into tasks");
                            setStatus("Working");
                            for(String task : children){
                                LOG.trace("New task: {}", task);
                                zk.getData("/assign/worker-" + serverId + "/" + task, false, cb, task);
                            }
                        }
                        
                    }.init(assignedTaskCache.addedAndSet(children),taskDataCallback));
                }
                break;
            default:
                LOG.error("get Children failed:" + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    DataCallback taskDataCallback = new DataCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                zk.getData(path, false, taskDataCallback, null);
                break;
            case OK:
                executor.execute(new Runnable(){

                   byte[] data;
                   Object ctx;
                    
                    public Runnable init(byte[] data, Object ctx){
                        this.data = data;
                        this.ctx = ctx;
                        return this;
                    }
                    
                    @Override
                    public void run() {
                        LOG.info("Executing your task: " + new String(data));
                        zk.create("/status/" + (String)ctx, "done".getBytes(), 
                                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                                taskStatusCreateCallback,null);
                        zk.delete("/assign/worker-" + serverId, -1, taskVoidCallback, null);
                    }
                    
                }.init(data,ctx));
                break;
            default:
                LOG.error("Failed to get task data:" + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    StringCallback taskStatusCreateCallback = new StringCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                zk.create(path + "/status", "done".getBytes(), 
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        taskStatusCreateCallback,null);;
                break;
            case OK:
                LOG.info("Created status znode correctly: " + name);
                break;
            case NODEEXISTS:
                LOG.warn("Node exists:" + path);
                break;
            default :
                LOG.error("Failed to create task data: " + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    VoidCallback taskVoidCallback = new VoidCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                break;
            case OK:
                LOG.info("Task correctly deleted: " + path);
                break;
            default:
                LOG.error("Failed to delete task data" + KeeperException.create(Code.get(rc), path));
            } 
        }
        
    };
    
    @Override
    public void close() throws IOException {
        LOG.info( "Closing" );
        try{
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("ZooKeeper interrupted while closing");
        }
    }
    
    public static void main(String[] args) throws Exception{
        String hostPort = "localhost:2181,localhost:2182,localhost:2183";
        Worker worker = new Worker(hostPort);
        worker.startZk();
        while(!worker.connected){
            Thread.sleep(100);
        }
        worker.bootstrap();
        worker.register();
        worker.getTasks();
        while(!worker.isExpired()){
            Thread.sleep(1000);
        }
    }

   
    
}
