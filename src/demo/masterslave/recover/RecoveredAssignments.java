package demo.masterslave.recover;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveredAssignments {

	private static final Logger LOG = LoggerFactory.getLogger(RecoveredAssignments.class);
	
	private List<String> tasks;
	private List<String> assignments;
	private List<String> status;
	private List<String> activeWorkers;
	private List<String> assignedWorkers;
	
	RecoveryCallback cb;
	
	ZooKeeper zk;
	
	public interface RecoveryCallback{
		final static int OK = 0;
		final static int FAILED = -1;
		
		public void recoveryComplete(int rc,List<String> tasks);
	}
	
	public RecoveredAssignments(ZooKeeper zk){
		this.zk = zk;
		this.assignments = new ArrayList<String>();
	}
	
	public void recover(RecoveryCallback cb){
		this.cb = cb;
		getTasks();
	}
	
	private void getTasks(){
		zk.getChildren("/tasks", false,taskCallback,null);
	}
	
	ChildrenCallback taskCallback = new ChildrenCallback(){

		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			switch(Code.get(rc)){
            case CONNECTIONLOSS : 
                getTasks();
                break;
            case OK:
                LOG.info("Get task for recover");
                tasks = children;
                getAssignedWorkers();
                break;
            default:
                LOG.error("get Children failed:" + KeeperException.create(Code.get(rc),path));
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }			
		}
		
	};
	
	private void getAssignedWorkers(){
		zk.getChildren("/assign", false,assignedWorkersCallback,null);
	}
	
	ChildrenCallback assignedWorkersCallback = new ChildrenCallback(){

		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			switch(Code.get(rc)){
            case CONNECTIONLOSS : 
            	getAssignedWorkers();
                break;
            case OK:
                assignedWorkers = children;
                getWorkers(children);
                break;
            default:
                LOG.error("get Children failed:" + KeeperException.create(Code.get(rc),path));
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }			
		}
		
	};
	
	private void getWorkers(Object ctx){
		zk.getChildren("/workers", false, workersCallback, ctx);
	}
	
	ChildrenCallback workersCallback = new ChildrenCallback(){

		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			switch(Code.get(rc)){
            case CONNECTIONLOSS : 
            	getWorkers(ctx);
                break;
            case OK:
            	LOG.info("Getting worker assignments for recovery: " + children.size());
            	if(children.size() == 0){
            		LOG.warn("Empty list of workers,possibily just starting.");
            		cb.recoveryComplete(RecoveryCallback.OK, new ArrayList<String>());
            		break;
            	}
            	activeWorkers = children;
            	for(String s: assignedWorkers){
            		getWorkerAssignments("/assign/" + s);
            	}
                break;
            default:
                LOG.error("get Children failed:" + KeeperException.create(Code.get(rc),path));
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }			
		}
		
	};
	
	private void getWorkerAssignments(String s){
		zk.getChildren(s, false, workerAssignmentsCallback, null);
	}
	
	ChildrenCallback workerAssignmentsCallback = new ChildrenCallback(){

		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			switch(Code.get(rc)){
            case CONNECTIONLOSS : 
            	getWorkerAssignments(path);
                break;
            case OK:
            	String worker = path.replace("/assign/", "");
            	if(activeWorkers.contains(worker)){
            		assignments.addAll(children);
            	}else{
            		for(String task : children){
            			if(tasks.contains(task)){
            				tasks.add(task);
            				getDataReassign(path,task);
            			}else{
            				deleteAssignment(path + "/" + task);
            			}
            			deleteAssignment(path);
            		}
            	}
            	assignedWorkers.remove(worker);
            	if(assignedWorkers.size() == 0){
            		LOG.info("Getting statuses for recovery");
            		getStatuses();
            	}
                break;
            case NONODE:
            	LOG.info( "No such znode exists: " + path );
            	break;
            default:
                LOG.error("get Children failed:" + KeeperException.create(Code.get(rc),path));
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }			
		}
		
	};
	
	void getDataReassign(String path,String task){
		zk.getData(path, false, getDataReassignCallback, task);
	}
	
	DataCallback getDataReassignCallback = new DataCallback(){

		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch(Code.get(rc)){
            case CONNECTIONLOSS : 
            	getDataReassign(path,(String)ctx);
                break;
            case OK:
            	recreateTask(new RecreateTaskCtx(path,(String)ctx,data));
                break;
            case NONODE:
            	LOG.info( "No such znode exists: " + path );
            	break;
            default:
                LOG.error("Something went wrong when getting data " + KeeperException.create(Code.get(rc),path));
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
		zk.create("/tasks/" + ctx.task, ctx.data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, recreateTaskCallback, ctx);
	}
	
	StringCallback recreateTaskCallback = new StringCallback(){

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch(Code.get(rc)){
            case CONNECTIONLOSS : 
            	recreateTask(new RecreateTaskCtx(path,(String)ctx,data));
                break;
            case OK:
            	deleteAssignment(((RecreateTaskCtx) ctx).path);
                break;
            case NODEEXISTS:
            	LOG.warn("Node shouldn't exist: " + path);
            	break;
            default:
                LOG.error("Something wwnt wrong when recreating task" + KeeperException.create(Code.get(rc),path));
            }			
			
		}
		
	};
	
	void deleteAssignment(String path){
		zk.delete(path, -1, taskDeletionCallback , null);
	}
	
	VoidCallback taskDeletionCallback  = new VoidCallback(){

		@Override
		public void processResult(int rc, String path, Object ctx) {
			switch(Code.get(rc)) {
            case CONNECTIONLOSS:
                deleteAssignment(path);
                break;
            case OK:
                LOG.info("Task correctly deleted: " + path);
                break;
            default:
                LOG.error("Failed to delete task data" + 
                        KeeperException.create(Code.get(rc), path));
            } 
		}
		
	};
	
	void getStatuses(){
		zk.getChildren("/status", false,statusCallback,null);
	}
	
	ChildrenCallback statusCallback = new ChildrenCallback(){

		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			switch(Code.get(rc)){
            case CONNECTIONLOSS : 
            	getWorkerAssignments(path);
                break;
            case OK:
            	String worker = path.replace("/assign/", "");
            	if(activeWorkers.contains(worker)){
            		assignments.addAll(children);
            	}else{
            		for(String task : children){
            			if(tasks.contains(task)){
            				tasks.add(task);
            				getDataReassign(path,task);
            			}else{
            				deleteAssignment(path + "/" + task);
            			}
            			deleteAssignment(path);
            		}
            	}
            	assignedWorkers.remove(worker);
            	if(assignedWorkers.size() == 0){
            		LOG.info("Getting statuses for recovery");
            		getStatuses();
            	}
                break;
            case NONODE:
            	LOG.info( "No such znode exists: " + path );
            	break;
            default:
                LOG.error("get Children failed:" + KeeperException.create(Code.get(rc),path));
                cb.recoveryComplete(RecoveryCallback.FAILED, null);
            }			
		}
		
	};
}
