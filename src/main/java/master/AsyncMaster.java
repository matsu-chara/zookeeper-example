package master;

import base.PrintWatcher;
import base.ZookeeperExecutor;
import base.ZookeeperRoleBase;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;

class AsyncMaster extends ZookeeperRoleBase implements Master {
    MasterStates state;

    public AsyncMaster(ZooKeeper zk) {
        super(zk);
    }

    public static void main(String args[]) throws Exception {
        ZookeeperExecutor exec         = new ZookeeperExecutor();
        exec.withZk(new PrintWatcher(), zk -> {
            AsyncMaster am = new AsyncMaster(zk);
            am.runForMaster();
            am.sleep(60);
        });
    }

    @Override public void runForMaster() {
        zk.create("/master",
                  serverId.getBytes(),
                  Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL,
                  (resultCode, path, context, name) -> {
                      switch (Code.get(resultCode)) {
                          case CONNECTIONLOSS:
                              checkMaster();
                              break;
                          case OK:
                              state = MasterStates.ELECTED;
                              takeLeadership();
                              break;
                          case NODEEXISTS:
                              state = MasterStates.NOTELECTED;
                              masterExists();
                              break;
                          default:
                              state = MasterStates.NOTELECTED;
                              Log.error("Something went wrong when running for master.", KeeperException.create(Code.get(resultCode), path));
                      }
                  },
                  null);
    }

    void takeLeadership() {
        Log.info("I am Leader!");
    }

    void checkMaster() {
        zk.getData("/master",
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
                               state = (new String(data).equals(serverId)) ? MasterStates.ELECTED : MasterStates.NOTELECTED;
                               break;
                       }
                   },
                   null);
    }

    void masterExists() {
        zk.exists("/master",
                  new MasterExistsWatcher(this),
                  (resultCode, path, context, stat) -> {
                      switch (Code.get(resultCode)) {
                          case CONNECTIONLOSS:
                              masterExists();
                              break;
                          case OK:
                              break;
                          case NONODE:
                              state = MasterStates.RUNNING;
                              runForMaster();
                              break;
                          default:
                              checkMaster();
                              break;
                      }
                  },
                  null);
    }
}
