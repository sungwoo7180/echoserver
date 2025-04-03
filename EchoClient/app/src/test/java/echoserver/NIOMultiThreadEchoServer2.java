package echoserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

public class NIOMultiThreadEchoServer2 {
    private static final int PORT = 12345;
    private static final ExecutorService readExecutor = Executors.newFixedThreadPool(5); // Read 스레드 풀
    private static final ExecutorService writeExecutor = Executors.newFixedThreadPool(5); // Write 스레드 풀

    public static void main(String[] args) throws IOException {

        try (ServerSocketChannel serverChannel = ServerSocketChannel.open();
             Selector selector = Selector.open()) {

            serverChannel.bind(new InetSocketAddress(PORT));
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("Echo server started on port " + PORT);

            while (true) {
                selector.select(); // I/O 이벤트 대기
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove(); // 현재 키 처리 예정이므로 목록에서 제거

                    if (!key.isValid()) {       // CancelledKeyException 방지 코드
                        key.cancel();
                        continue;               // 유효하지 않은 키는 무시하고 다시 while 문 처음으로
                    }
                    try {
                        if (key.isAcceptable()) {
                            handleAccept(key, selector);
                        } else if (key.isReadable()) {
                            readExecutor.submit(() -> handleRead(key)); // Read 작업을 스레드 풀에서 처리
                        } else if (key.isWritable()) {
                            writeExecutor.submit(() -> handleWrite(key)); // Write 작업을 스레드 풀에서 처리
                        }
                    } catch (IOException ex) {
                        cleanupKey(key);
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
            clientChannel.register(selector, SelectionKey.OP_READ);
            System.out.println("Client connected: " + clientChannel.getRemoteAddress());
        }
    }

    private static void handleRead(SelectionKey key) {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try {
            int bytesRead = clientChannel.read(buffer);
            if (bytesRead == -1) {
                cleanupKey(key);
                return;
            }

            if (bytesRead > 0) {
                buffer.flip();
                clientChannel.write(buffer);
                buffer.clear();
            }
        } catch (IOException ex) {
            cleanupKey(key);
        }
    }

    private static void handleWrite(SelectionKey key) {
        // Write logic (Handle the response)
    }

    private static void cleanupKey(SelectionKey key) {
        try {
            key.cancel();
            key.channel().close();
        } catch (IOException ignore) {
        }
    }
}
