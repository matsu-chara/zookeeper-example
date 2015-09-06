package master.roles;

import base.ZookeeperRoleBase;
import master.MasterStates;
import master.watcer.MasterExistsWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class MasterElector extends ZookeeperRoleBase {
    MasterStates state = MasterStates.NOTELECTED;

    public MasterElector(ZooKeeper zk, Class clazz) {
        super(zk, clazz);
    }

    public void runForMaster() {
        zk.create("/master",
                  serverId.getBytes(),
                  ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL,
                  (resultCode, path, context, name) -> {
                      switch (KeeperException.Code.get(resultCode)) {
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
                              Log.error("Something went wrong when running for masterRunner.", KeeperException.create(KeeperException.Code.get(resultCode), path));
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
                       switch (KeeperException.Code.get(resultCode)) {
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
                      switch (KeeperException.Code.get(resultCode)) {
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
