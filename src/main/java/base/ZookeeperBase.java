package base;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class ZookeeperBase implements Watcher {
    protected final String serverId = Integer.toHexString((new Random()).nextInt());
    protected final Logger Log;

    public ZookeeperBase() {
        Log = LoggerFactory.getLogger(this.getClass());
    }

    // InterruptedExceptionが検査例外であるため、lambdaの中で直接Thread.sleepを使うことができない。
    // 対策のためここで非検査例外に変換。
    protected void sleep(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override public void process(WatchedEvent event) {
        System.out.println(event.toString());
    }
}

