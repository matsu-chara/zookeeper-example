package base;

import java.util.ArrayList;
import java.util.List;

public class ChildrenCache {
    List<String> children;

    public ChildrenCache(List<String> children) {
        this.children = children;
    }

    public List<String> removeAndSet(List<String> newChildren) {
        List<String> removed = new ArrayList<>();

        children.stream()
                .filter(s -> !newChildren.contains(s))
                .forEach(removed::add);

        children = newChildren;
        return removed;
    }
}
