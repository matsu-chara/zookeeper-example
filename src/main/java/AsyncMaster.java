import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class AsyncMaster implements Watcher {
    private static final Logger Log = LoggerFactory.getLogger(AsyncMaster.class);

    ZooKeeper zk;
    String    hostPort;
    Random  random   = new Random();
    String  serverId = Integer.toHexString(random.nextInt());
    boolean isLeader = false;

    public AsyncMaster(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    void runForMaster() {
        zk.create("/master",
                  serverId.getBytes(),
                  Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL,
                  new StringCallback() {
                      @Override public void processResult(int rc, String path, Object ctx, String name) {
                          switch (Code.get(rc)) {
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
                          System.out.println("I'm " + (isLeader ? "" : "*not* ") + "the leader");
                      }
                  },
                  null);
    }

    void checkMaster() {
        zk.getData("/master",
                   false,
                   new DataCallback() {
                       @Override public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                           switch (Code.get(rc)) {
                               case CONNECTIONLOSS:
                                   checkMaster();
                                   break;
                               case NONODE:
                                   runForMaster();
                                   break;
                           }
                       }
                   },
                   null);
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
                  Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT,
                  new StringCallback() {
                      @Override public void processResult(int rc, String path, Object ctx, String name) {
                          switch (Code.get(rc)) {
                              case CONNECTIONLOSS:
                                  createParent(path, (byte[]) ctx);
                                  break;
                              case OK:
                                  Log.info("Parent created");
                                  break;
                              case NODEEXISTS:
                                  Log.warn("Parent already registered: " + path);
                                  break;
                              default:
                                  Log.error("Something went wrong: ", KeeperException.create(Code.get(rc), path));
                          }
                      }
                  },
                  data);
    }

    @Override public void process(WatchedEvent event) {
        Log.info(event.toString() + ", " + hostPort);
    }

    public static void main(String args[]) throws Exception {
        AsyncMaster am = new AsyncMaster(args[0]);
        am.startZk();

        am.runForMaster();
        Thread.sleep(10 * 1000);
        am.stopZK();
    }
}
