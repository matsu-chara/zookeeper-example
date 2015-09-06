import base.ZookeeperBase;
import base.ZookeeperExecutor;
import org.apache.zookeeper.*;

public class Bootstrap extends ZookeeperBase {
    public static void main(String args[]) throws Exception {
        ZookeeperExecutor exec = new ZookeeperExecutor();
        Bootstrap b = new Bootstrap();

        exec.withZk(b, zk -> {
            b.bootstrap(zk);
            b.sleep(3);
        });
    }

    void bootstrap(ZooKeeper zk) {
        createParent(zk, "/workers", new byte[0]);
        createParent(zk, "/assign", new byte[0]);
        createParent(zk, "/tasks", new byte[0]);
        createParent(zk, "/status", new byte[0]);
    }

    void createParent(ZooKeeper zk, String path, byte[] data) {
        zk.create(path,
                  data,
                  ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT,
                  new AsyncCallback.StringCallback() {
                      @Override public void processResult(int rc, String path, Object ctx, String name) {
                          switch (KeeperException.Code.get(rc)) {
                              case CONNECTIONLOSS:
                                  createParent(zk, path, (byte[]) ctx);
                                  break;
                              case OK:
                                  Log.info("Parent created");
                                  break;
                              case NODEEXISTS:
                                  Log.warn("Parent already registered: " + path);
                                  break;
                              default:
                                  Log.error("Something went wrong: ", KeeperException.create(KeeperException.Code.get(rc), path));
                          }
                      }
                  },
                  data);
    }

}
