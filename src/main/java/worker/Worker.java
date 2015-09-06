package worker;

import base.ZookeeperRoleBase;
import base.ZookeeperExecutor;
import base.PrintWatcher;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;

public class Worker extends ZookeeperRoleBase {
    private String status = "Idle";

    public Worker(ZooKeeper zk) {
        super(zk);
    }

    public static void main(String[] args) throws Exception {
        ZookeeperExecutor exec    = new ZookeeperExecutor();
        PrintWatcher watcher = new PrintWatcher();

        exec.withZk(watcher, zk -> {
            Worker w = new Worker(zk);
            w.register();
            w.sleep(6);
        });
    }

    void register() {
        zk.create("/workers/worker-" + serverId,
                  "Idle".getBytes(),
                  Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL,
                  (resultCode, path, context, name) -> {
                      switch (Code.get(resultCode)) {
                          case CONNECTIONLOSS:
                              register();
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

    synchronized void updateStatus(String status) {
        if (status.equals(this.status)) {
            zk.setData("/workers/worker-" + serverId,
                       status.getBytes(),
                       -1,
                       (AsyncCallback.StatCallback) (resultCode, path, context, stat) -> {
                           switch (Code.get(resultCode)) {
                               case CONNECTIONLOSS:
                                   updateStatus((String) context);
                           }
                       },
                       status);
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

}
