package master;

import base.ZookeeperBase;
import base.ZookeeperExecutor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class AsyncMaster extends ZookeeperBase {
    boolean isLeader = false;

    public static void main(String args[]) throws Exception {
        ZookeeperExecutor exec = new ZookeeperExecutor();
        AsyncMaster       am   = new AsyncMaster();

        exec.withZk(am, zk -> {
            am.runForMaster(zk);
            am.sleep(10);
        });
    }

    void runForMaster(ZooKeeper zk) {
        zk.create("/master",
                  serverId.getBytes(),
                  Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL,
                  (resultCode, path, context, name) -> {
                      switch (Code.get(resultCode)) {
                          case CONNECTIONLOSS:
                              checkMaster(zk);
                              return;
                          case OK:
                              isLeader = true;
                              break;
                          default:
                              isLeader = false;
                              break;
                      }
                      System.out.println("I'm " + (isLeader ? "" : "*not* ") + "the leader");
                  },
                  null);
    }

    void checkMaster(ZooKeeper zk) {
        zk.getData("/master",
                   false,
                   (resultCode, path, context, data, stat) -> {
                       switch (Code.get(resultCode)) {
                           case CONNECTIONLOSS:
                               checkMaster(zk);
                               break;
                           case NONODE:
                               runForMaster(zk);
                               break;
                           case OK:
                               isLeader = new String(data).equals(serverId);
                               break;
                       }
                   },
                   null);
    }
}
