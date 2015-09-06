package master;

import base.ZookeeperExecutor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import base.ZookeeperBase;

public class Master extends ZookeeperBase {
    boolean isLeader = false;

    public static void main(String args[]) throws Exception {
        ZookeeperExecutor exec = new ZookeeperExecutor();
        Master            m    = new Master();

        exec.withZk(m, zk -> {
            m.runForMaster(zk);
            if (m.isLeader) {
                System.out.println("I'm the leader");
                m.sleep(10);
            } else {
                System.out.println("Someone else is the leader");
            }
        });
    }

    void runForMaster(ZooKeeper zk) {
        while (true) {
            try {
                zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (NoNodeException e) {
                isLeader = false;
                break;
            } catch (KeeperException ignored) {

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (checkMaster(zk)) break;
        }
    }

    boolean checkMaster(ZooKeeper zk) {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (NoNodeException e) {
                return false;
            } catch (KeeperException ignored) {

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
