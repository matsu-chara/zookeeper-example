package chapter3.base;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class PrintWatcher implements Watcher {
    @Override public void process(WatchedEvent event) {
        System.out.println(event.toString());
    }
}
