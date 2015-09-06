package worker;

import base.PrintWatcher;
import base.ZookeeperExecutor;
import base.ZookeeperRoleBase;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

class Worker extends ZookeeperRoleBase {
    private       String                status       = "Idle";
    private       Executor              executor     = Executors.newFixedThreadPool(3);
    private final BlockingQueue<String> onGoingTasks = new LinkedBlockingQueue<>();

    public Worker(ZooKeeper zk) {
        super(zk);
    }

    public static void main(String[] args) throws Exception {
        ZookeeperExecutor exec    = new ZookeeperExecutor();
        PrintWatcher      watcher = new PrintWatcher();

        exec.withZk(watcher, zk -> {
            Worker w = new Worker(zk);
            w.register();
            w.sleep(20);
        });
    }

    void register() {
        zk.create("/workers/worker-" + serverId,
                  new byte[0],
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
                              Log.error("Something went wrong: ", KeeperException.create(Code.get(resultCode), path));
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

    public void getTasks() {
        zk.getChildren("/assign/worker-" + serverId,
                       new TaskWatcher(this, serverId),
                       (resultCode, path, context, children) -> {
                           switch (Code.get(resultCode)) {
                               case CONNECTIONLOSS:
                                   getTasks();
                                   break;
                               case OK:
                                   if (children != null) {
                                       executor.execute(() -> {
                                           Log.info("Looping into tasks");
                                           synchronized (onGoingTasks) {
                                               for (String task : children) {
                                                   if (!onGoingTasks.contains(task)) {
                                                       Log.trace("New task: " + task);
                                                       zk.getData("/assign/worker-" + serverId + "/" + task,
                                                                  false,
                                                                  (resultCode2, path2, context2, data, stat) -> {
                                                                      onGoingTasks.add(task);
                                                                      // TODO: process task
                                                                  },
                                                                  task);
                                                   }
                                               }
                                           }
                                       });
                                   }
                                   break;
                               default:
                                   Log.error("getChildren failed: ",
                                             KeeperException.create(Code.get(resultCode), path));
                           }
                       },
                       null);
    }
}
