import org.apache.zookeeper.WatchedEvent;

public interface Watcher {
    void process(WatchedEvent event);
}