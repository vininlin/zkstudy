/**
 * 
 */
package demo.masterslave;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
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
 * @createDate 2016-9-1
 * 
 */
public class Client implements Watcher,Closeable{

    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    ZooKeeper zk;
    String hostPort;
    volatile boolean connected = false;
    volatile boolean expired = false;
    
    public Client(String hostPort){
        this.hostPort = hostPort;
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
    
    void startZk() throws IOException{
        zk = new ZooKeeper(hostPort,15000,this);
    }
    
    void submitTask(String task,TaskObject taskCtx){
        taskCtx.setTask(task);
        zk.create("/tasks/task-", task.getBytes(), 
                Ids.OPEN_ACL_UNSAFE, 
                CreateMode.PERSISTENT_SEQUENTIAL, createTaskCallback, taskCtx);
    }
    
    StringCallback createTaskCallback = new StringCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                submitTask(((TaskObject)ctx).getTask(),(TaskObject)ctx);
                break;
            case OK:
                LOG.info("My created task name: " + name);
                ((TaskObject)ctx).setTaskName(name);
                watchStatus(name.replace("/tasks/", "/status/"),ctx);
                break;
            case NODEEXISTS:
                LOG.warn("Assign node Aready create");
                break;
            default :
                LOG.error("Something went wrong:" + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    protected ConcurrentHashMap<String, Object> ctxMap = new ConcurrentHashMap<String, Object>();
    
    void watchStatus(String path,Object ctx){
        ctxMap.put(path, ctx);
        zk.exists(path, statusWatcher,existsCallback,ctx);
    }
    
    Watcher statusWatcher = new Watcher(){

        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == EventType.NodeCreated){
                assert event.getPath().contains("/status/task-");
                assert ctxMap.containsKey(event.getPath());
                zk.getData(event.getPath(), false, getDataCallback, ctxMap.get(event.getPath()));
            }
        }
        
    };
    
    StatCallback existsCallback = new StatCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                watchStatus(path,ctx);
                break;
            case OK:
                if(stat != null){
                    zk.getData(path, false, getDataCallback, ctx);
                    LOG.info("Status node is there: " + path);
                }
                break;
            case NONODE:
                break;
            default :
                LOG.error("Something went wrong when checking if the status node exists:" + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    DataCallback getDataCallback = new DataCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                zk.getData(path, false, getDataCallback, ctx);
                break;
            case OK:
                String taskResult = new String(data);
                LOG.info("Task " + path + ", " + taskResult);
                assert(ctx != null);
                ((TaskObject)ctx).setStatus(taskResult.contains("done"));
                zk.delete(path, -1, taskDeleteCallback, null);
                ctxMap.remove(path);
                break;
            case NONODE:
                LOG.warn("Status node is gone!");
                return;
            default :
                LOG.error("Something went wrong here," + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    VoidCallback taskDeleteCallback = new VoidCallback(){

        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                zk.delete(path, -1, taskDeleteCallback, null);
                break;
            case OK:
                LOG.info("Successfully deleted " + path);
                break;
            default :
                LOG.error("Something went wrong here," + KeeperException.create(Code.get(rc),path));
            }
        }
        
    };
    
    public static void main(String[] args) throws Exception{
        String hostPort = "localhost:2181,localhost:2182,localhost:2183";
        Client c = new Client(hostPort);
        c.startZk();
        while(!c.connected){
            Thread.sleep(100);
        }
        TaskObject task1 = new TaskObject();
        TaskObject task2 = new TaskObject();
        
        c.submitTask("sample task",task1);
        c.submitTask("Another sample task", task2);
        
        task1.awaitUntilDown();
        task2.awaitUntilDown();
    }

   
    @Override
    public void close() throws IOException {
        LOG.info( "Closing" );
        try{
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("ZooKeeper interrupted while closing");
        }
        
    }
    
    static class TaskObject{
        
        private String task;
        private String taskName;
        private boolean done = false;
        private boolean successful = false;
        private CountDownLatch latch = new CountDownLatch(1);
       
        public String getTask() {
            return task;
        }
       
        public void setTask(String task) {
            this.task = task;
        }
        
        public String getTaskName() {
            return taskName;
        }
        
        public void setTaskName(String taskName) {
            this.taskName = taskName;
        }
        
        public void setStatus(boolean status){
            this.successful = status;
            this.done = true;
            latch.countDown();
        }
        
        void awaitUntilDown(){
            try {
                latch.wait();
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException while waiting for task to get done");
            }
        }
        
        synchronized boolean isDone(){
            return done;
        }
        
        synchronized boolean isSuccessful(){
            return successful;
        }
    }

}
