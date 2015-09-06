package chapter3;

import chapter3.base.ZookeeperRoleBase;
import chapter3.base.PrintWatcher;
import chapter3.base.ZookeeperExecutor;
import org.apache.zookeeper.*;

class Bootstrap extends ZookeeperRoleBase {
    public Bootstrap(ZooKeeper zk) {
        super(zk);
    }

    public static void main(String args[]) throws Exception {
        ZookeeperExecutor exec    = new ZookeeperExecutor();
        PrintWatcher      watcher = new PrintWatcher();

        exec.withZk(watcher, zk -> {
            Bootstrap b = new Bootstrap(zk);
            b.bootstrap();
            b.sleep(3);
        });
    }

    void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zk.create(path,
                  data,
                  ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT,
                  (resultCode, path1, context, name) -> {
                      switch (KeeperException.Code.get(resultCode)) {
                          case CONNECTIONLOSS:
                              createParent(path1, (byte[]) context);
                              break;
                          case OK:
                              Log.info("Parent created");
                              break;
                          case NODEEXISTS:
                              Log.warn("Parent already registered: " + path1);
                              break;
                          default:
                              Log.error("Something went wrong: ", KeeperException.create(KeeperException.Code.get(resultCode), path1));
                      }
                  },
                  data);
    }

}
