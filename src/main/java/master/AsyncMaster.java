package master;

import base.PrintWatcher;
import base.ZookeeperExecutor;
import base.ZookeeperRoleBase;
import master.masterElector.MasterElector;
import master.taskAssigner.TaskAssigner;
import master.workerController.WorkerController;
import org.apache.zookeeper.ZooKeeper;

class AsyncMaster extends ZookeeperRoleBase {
    MasterElector    masterElector;
    WorkerController workerController;
    TaskAssigner taskAssigner;

    public AsyncMaster(ZooKeeper zk) {
        super(zk);
        masterElector = new MasterElector(zk, Log, serverId);
        workerController = new WorkerController(zk, Log);
        taskAssigner = new TaskAssigner(zk, Log);
    }

    public static void main(String args[]) throws Exception {
        ZookeeperExecutor exec = new ZookeeperExecutor();
        exec.withZk(new PrintWatcher(), zk -> {
            AsyncMaster am = new AsyncMaster(zk);
            am.masterElector.runForMaster();
            am.sleep(60);
        });
    }
}

