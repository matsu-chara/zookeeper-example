import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class Worker implements Watcher {
    private static final Logger Log = LoggerFactory.getLogger(Worker.class);

    ZooKeeper zk;
    String    hostPort;
    Random random   = new Random();
    String serverId = Integer.toHexString(random.nextInt());

    public Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }

    @Override public void process(WatchedEvent event) {
        Log.info(event.toString() + ", " + hostPort);
    }

    void register() {
        zk.create("/workers/worker-" + serverId,
                  "Idle".getBytes(),
                  Ids.OPEN_ACL_UNSAFE,
                  CreateMode.EPHEMERAL,
                  new AsyncCallback.StringCallback() {
                      @Override public void processResult(int rc, String path, Object ctx, String name) {
                          switch (Code.get(rc)) {
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
                                  Log.error("Something went wrong: " + KeeperException.create(Code.get(rc), path));
                          }
                      }
                  }
                , null);
    }

    public static void main(String[] args) throws Exception {
        Worker w = new Worker(MyZooKeeperConst.hostPort);
        w.startZk();
        w.register();
        Thread.sleep(6 * 1000);
    }
}
