import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.*;

import java.io.IOException;
import java.util.Random;

public class Master implements Watcher {
    ZooKeeper zk;
    String    hostPort;
    Random  random   = new Random();
    String  serverId = Integer.toHexString(random.nextInt());
    boolean isLeader = false;

    public Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void stopZK() throws Exception {
        zk.close();
    }

    void runForMaster() throws InterruptedException {
        while (true) {
            try {
                zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (NoNodeException e) {
                isLeader = false;
                break;
            } catch (KeeperException ignored) {}
            if (checkMaster()) break;
        }
    }

    boolean checkMaster() throws InterruptedException {
        while(true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (NoNodeException e) {
                return false;
            } catch(KeeperException ignored) {}
        }
    }

    @Override public void process(WatchedEvent event) {
        System.out.println(event);
    }

    public static void main(String args[]) throws Exception {
        Master m = new Master(MyZooKeeperConst.hostPort);
        m.startZk();

        m.runForMaster();

        if(m.isLeader) {
            System.out.println("I'm the leader");
            Thread.sleep(10 * 1000);
        } else {
            System.out.println("Someone else is the leader");
        }

        m.stopZK();
    }
}
