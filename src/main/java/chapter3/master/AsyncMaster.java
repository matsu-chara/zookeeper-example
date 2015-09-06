package chapter3.master;

import chapter3.base.ZookeeperRoleBase;
import chapter3.base.ZookeeperExecutor;
import chapter3.base.PrintWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

class AsyncMaster extends ZookeeperRoleBase {
    boolean isLeader = false;

    public AsyncMaster(ZooKeeper zk) {
        super(zk);
    }

    public static void main(String args[]) throws Exception {
        ZookeeperExecutor       exec    = new ZookeeperExecutor();
        PrintWatcher watcher = new PrintWatcher();

        exec.withZk(watcher, zk -> {
            AsyncMaster am = new AsyncMaster(zk);
            am.runForMaster();
            am.sleep(60);
        });
    }

    void runForMaster() {
        zk.create("/chapter3/master",
                  serverId.getBytes(),
                  Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL,
                  (resultCode, path, context, name) -> {
                      switch (Code.get(resultCode)) {
                          case CONNECTIONLOSS:
                              checkMaster();
                              return;
                          case OK:
                              isLeader = true;
                              break;
                          default:
                              isLeader = false;
                              break;
                      }
                      Log.info("I'm " + (isLeader ? "" : "*not* ") + "the leader");
                  },
                  null);
    }

    void checkMaster() {
        zk.getData("/chapter3/master",
                   false,
                   (resultCode, path, context, data, stat) -> {
                       switch (Code.get(resultCode)) {
                           case CONNECTIONLOSS:
                               checkMaster();
                               break;
                           case NONODE:
                               runForMaster();
                               break;
                           case OK:
                               isLeader = new String(data).equals(serverId);
                               break;
                       }
                   },
                   null);
    }
}