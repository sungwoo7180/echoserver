// ChatServer.java
package echoserver.main;

import echoserver.main.domain.ClientInfo;
import echoserver.main.handler.ChatHandler;
import echoserver.main.service.NicknameService;
import echoserver.main.service.BroadcastService;

import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class ChatServer {
    private static final int PORT = 12345;
    private static final int POOL_SIZE = 10;
    public static final ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
    public static final AtomicInteger threadCounter = new AtomicInteger(1);
    public static final Set<String> allUsers = new ConcurrentSkipListSet<>();
    public static final Set<String> activeUsers = new ConcurrentSkipListSet<>();

    public static FileOutputStream nicknameOutputStream;
    public static FileOutputStream chatOutputStream;

    public static NicknameService nicknameService;
    public static BroadcastService broadcastService;

    private static final String TEXTPATH = "app/src/test/java/echoserver/main/text/chathistory.txt";
    private static final String NICKNAMEPATH = "app/src/test/java/echoserver/main/text/nickname.txt";

    public static void main(String[] args) {
        try (
                ServerSocketChannel serverChannel = ServerSocketChannel.open();
                Selector selector = Selector.open()
        ) {
            nicknameOutputStream = new FileOutputStream(NICKNAMEPATH, true);
            chatOutputStream = new FileOutputStream(TEXTPATH, true);

            nicknameService = new NicknameService(allUsers, activeUsers, nicknameOutputStream, TEXTPATH);
            broadcastService = new BroadcastService();

            serverChannel.bind(new InetSocketAddress(PORT));
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            while (true) {
                selector.select();
                var iter = selector.selectedKeys().iterator();
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
                        workerPool.submit(() -> ChatHandler.handle(key));
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("서버 오류: " + e.getMessage());
        }
    }

    private static void handleAccept(SelectionKey key, Selector selector) throws Exception {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();

        if (client != null) {
            client.configureBlocking(false);
            client.register(selector, SelectionKey.OP_READ, new ClientInfo(1024));
            System.out.println("[ACCEPT] 클라이언트 연결 수락: " + client.getRemoteAddress());
        }
    }
}