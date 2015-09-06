package master.workerController;

import base.ChildrenCache;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;

import java.util.List;

public class WorkerController {
    ZooKeeper     zk;
    Logger        Log;
    ChildrenCache workersCache;

    public WorkerController(ZooKeeper zk, Logger Log) {
        this.zk = zk;
        this.Log = Log;
    }

    public void getWorkers() {
        zk.getChildren("/workers",
                       new WorkersChangeWatcher(this),
                       (resultCode, path, context, children) -> {
                           switch (KeeperException.Code.get(resultCode)) {
                               case CONNECTIONLOSS:
                                   getWorkers();
                                   break;
                               case OK:
                                   Log.info("Successfully got a list of workers: " + children.size() + " workers");
                                   reassignAndSet(children);
                                   break;
                               default:
                                   Log.error("getChildren failed",
                                             KeeperException.create(KeeperException.Code.get(resultCode), path));
                           }
                       },
                       null);
    }

    void reassignAndSet(List<String> children) {
        List<String> toProcess;

        if(workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            Log.info("Removing and setting");
            toProcess = workersCache.removeAndSet(children);
        }

        if(toProcess != null) {
            for(String worker: toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }

    private void getAbsentWorkerTasks(String worker) {

    }

}
