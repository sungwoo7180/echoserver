// SelectorThread.java
package echoserver.main;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

public class SelectorThread implements Runnable {
    private final Selector selector;
    private final ExecutorService workerPool;

    public SelectorThread(Selector selector, ExecutorService workerPool) {
        this.selector = selector;
        this.workerPool = workerPool;
    }

    @Override
    public void run() {
        System.out.println("[SelectorThread] 시작됨");
        while (true) {
            try {
                selector.select();
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    System.out.println(key);
                    iter.remove();

                    if (!key.isValid()) {
                        key.cancel();
                        continue;
                    }

                    if (key.isAcceptable()) {
                        ChatServer2.handleAccept(key, selector);
                    } else if (key.isReadable()) {
                        workerPool.submit(() -> ChatServer2.handleReadWrite(key));
                    }
                }
            } catch (IOException e) {
                System.err.println("[SelectorThread] 에러: " + e.getMessage());
            }
        }
    }
}
