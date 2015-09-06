package master.taskAssigner;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public class TaskChangeWatcher implements Watcher {
    TaskAssigner taskAssigner;

    public TaskChangeWatcher(TaskAssigner taskAssigner) {
        this.taskAssigner = taskAssigner;
    }

    @Override public void process(WatchedEvent event) {
        if (event.getType() == EventType.NodeChildrenChanged) {
            assert "/tasks".equals(event.getPath());
            taskAssigner.getTasks();
        }
    }
}
