package worker;

import base.ZookeeperBase;
import base.ZookeeperExecutor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class Worker extends ZookeeperBase {
    public static void main(String[] args) throws Exception {
        ZookeeperExecutor exec = new ZookeeperExecutor();
        Worker w = new Worker();

        exec.withZk(w, zk -> {
            w.register(zk);
            w.sleep(6);
        });
    }

    void register(ZooKeeper zk) {
        zk.create("/workers/worker-" + serverId,
                  "Idle".getBytes(),
                  Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL,
                  (resultCode, path, context, name) -> {
                      switch (Code.get(resultCode)) {
                          case CONNECTIONLOSS:
                              register(zk);
                              break;
                          case OK:
                              Log.info("Resistered successfully: " + serverId);
                              break;
                          case NODEEXISTS:
                              Log.warn("Already registered: " + serverId);
                              break;
                          default:
                              Log.error("Something went wrong: " + KeeperException.create(Code.get(resultCode), path));
                      }
                  }
                , null);
    }
}
