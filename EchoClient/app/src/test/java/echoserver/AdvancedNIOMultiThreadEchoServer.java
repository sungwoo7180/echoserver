package echoserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

public class AdvancedNIOMultiThreadEchoServer {
    private static final int PORT = 12345;
    private static final int BUFFER_SIZE = 1024;
    private static final int POOL_SIZE = 5;

    private static final ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
    private static final AtomicInteger threadCounter = new AtomicInteger(1); // 스레드 번호 부여기
    private static final Map<Long, Integer> threadIdMap = new ConcurrentHashMap<>(); // 실제 Thread ID -> 넘버링

    public static void main(String[] args) throws IOException {

        try (ServerSocketChannel serverChannel = ServerSocketChannel.open();
             Selector selector = Selector.open()) {

            serverChannel.bind(new InetSocketAddress(PORT));
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("[MAIN] Echo server started on port " + PORT);

            while (true) {
                selector.select(); // I/O 이벤트 대기
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();

                    if (!key.isValid()) {
                        key.cancel();
                        continue;
                    }

                    if (key.isAcceptable()) {
                        handleAccept(key, selector);
                    } else if (key.isReadable()) {
                        workerPool.submit(() -> handleReadWrite(key));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleAccept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = server.accept();

        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            clientChannel.register(selector, SelectionKey.OP_READ, buffer);
            System.out.println("[ACCEPT] 클라이언트 연결 수락: " + clientChannel.getRemoteAddress());
        }
    }

    private static void handleReadWrite(SelectionKey key) {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        long threadId = Thread.currentThread().getId();
        int threadNum = threadIdMap.computeIfAbsent(threadId, id -> threadCounter.getAndIncrement());

        synchronized (key) {
            try {
                int bytesRead = clientChannel.read(buffer);
                if (bytesRead == -1) {
                    System.out.println("[Thread " + threadNum + "] 클라이언트 연결 종료: " + clientChannel.getRemoteAddress());
                    cleanupKey(key);
                    return;
                }

                buffer.flip();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                String message = new String(bytes).trim();

                if (!message.isEmpty()) {
                    System.out.println("[Thread " + threadNum + "] 클라이언트(" + clientChannel.getRemoteAddress() + ") 메시지: " + message);
                    ByteBuffer responseBuffer = ByteBuffer.wrap(("Echo: " + message + "\n").getBytes());
                    while (responseBuffer.hasRemaining()) {
                        clientChannel.write(responseBuffer);
                    }
                    System.out.println("[Thread " + threadNum + "] 클라이언트(" + clientChannel.getRemoteAddress() + ")에게 응답 전송 완료");
                }

                buffer.clear();
            } catch (IOException e) {
                cleanupKey(key);
            }
        }
    }

    private static void cleanupKey(SelectionKey key) {
        try {
            System.out.println("[CLEANUP] 연결 정리 중: " + ((SocketChannel) key.channel()).getRemoteAddress());
            key.cancel();
            key.channel().close();
        } catch (IOException ignore) {
        }
    }
}