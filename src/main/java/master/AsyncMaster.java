package master;

import base.PrintWatcher;
import base.ZookeeperExecutor;
import base.ZookeeperRoleBase;
import master.roles.MasterElector;
import master.roles.WorkerController;
import org.apache.zookeeper.ZooKeeper;

class AsyncMaster extends ZookeeperRoleBase {
    MasterElector    masterElector;
    WorkerController workerController;

    public AsyncMaster(ZooKeeper zk) {
        super(zk, AsyncMaster.class);
        masterElector = new MasterElector(zk, AsyncMaster.class);
        workerController = new WorkerController(zk, AsyncMaster.class);
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

