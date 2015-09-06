package master.taskAssigner;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TaskAssigner {
    ZooKeeper zk;
    Logger    Log;
    List<String> workerList = new ArrayList<>();

    public TaskAssigner(ZooKeeper zk, Logger log) {
        this.zk = zk;
        Log = log;
    }

    public void getTasks() {
        zk.getChildren("/tasks",
                       new TaskChangeWatcher(this),
                       (resultCode, path, context, tasks) -> {
                            switch(KeeperException.Code.get(resultCode)) {
                                case CONNECTIONLOSS:
                                    getTasks();
                                    break;
                                case OK:
                                    if(tasks != null) {
                                        assignTasks(tasks);
                                    }
                                    break;
                                default:
                                    Log.error("getChildren failed.",
                                              KeeperException.create(KeeperException.Code.get(resultCode), path));
                            }
                       },
                       null);
    }

    void assignTasks(List<String> tasks) {
        tasks.forEach(this::getTaskData);
    }

    void getTaskData(String task) {
        zk.getData("/tasks/" + task,
                   false,
                   (resultCode, path, context, data, stat) -> {
                       switch (KeeperException.Code.get(resultCode)) {
                           case CONNECTIONLOSS:
                               getTaskData((String) context);
                               break;
                           case OK:
                               // ランダムなworker選択
                               int worker = (new Random()).nextInt(workerList.size());
                               String designatedWorker = workerList.get(worker);

                               String assignmentPath = "/assign/" + designatedWorker + "/" + (String) context;
                               createAssignment(assignmentPath, data);
                           default:
                               Log.error("Error when trying to get task data.",
                                         KeeperException.create(KeeperException.Code.get(resultCode), path));
                       }
                   },
                   null);
    }

    void createAssignment(String assignmentPath, byte[] data) {
        zk.create(assignmentPath,
                  data,
                  ZooDefs.Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT,
                  (resultCode, path, context, name) -> {
                      switch (KeeperException.Code.get(resultCode)) {
                          case CONNECTIONLOSS:
                              createAssignment(path, (byte[]) context);
                              break;
                          case OK:
                              Log.info("Task assigned correctly: " + name);
                              deleteTask(name.substring(name.lastIndexOf("/") + 1));
                              break;
                          case NODEEXISTS:
                              Log.warn("Task already assigned");
                              break;
                          default:
                              Log.error("Error when trying to assign task.",
                                        KeeperException.create(KeeperException.Code.get(resultCode), path));
                      }
                  },
                  data);
    }

    void deleteTask(String task) {
        Log.error("not implemented yet");
    }
}
