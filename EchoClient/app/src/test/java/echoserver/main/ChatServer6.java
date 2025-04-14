package echoserver.main;

import echoserver.main.domain.ClientInfo;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ChatServer6 {
    private static final int PORT = 12345;
    private static final int POOL_SIZE = 10;
    private static final int BUFFER_SIZE = 1024;
    private static final String TEXTPATH = "app/src/test/java/echoserver/main/text/chathistory.txt";
    private static final String NICKNAMEPATH = "app/src/test/java/echoserver/main/text/nickname.txt";

    private static final ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
    private static final AtomicInteger threadCounter = new AtomicInteger(1);
    private static final Map<Long, Integer> threadIdMap = new ConcurrentHashMap<>();
    private static final Set<String> allUsers = new ConcurrentSkipListSet<>();
    private static final Set<String> activeUsers = new ConcurrentSkipListSet<>();
    private static FileOutputStream nicknameOutputStream;
    private static FileOutputStream chatOutputStream;

    public static void main(String[] args) {
        try (
                ServerSocketChannel serverChannel = ServerSocketChannel.open();
                Selector selector = Selector.open()
        ) {
            nicknameOutputStream = new FileOutputStream(NICKNAMEPATH, true);
            chatOutputStream = new FileOutputStream(TEXTPATH, true);
            onLoad();

            serverChannel.bind(new InetSocketAddress(PORT));
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("Chatting server started on port: " + PORT);

            while (true) {
                selector.select();
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
                        key.interestOps(0);
                        Future<String> future = workerPool.submit(() -> {
                            System.out.println("[<1>] handleReadWrite 실행 시작");  //
                            try {
                                return handleReadWrite(key); // <1> 메소드 리턴
                            } finally {
                                System.out.println("[<2>] finally 블록 진입 - interestOps 설정");
                                if (key.isValid()) {
                                    key.interestOps(SelectionKey.OP_READ);
                                    key.selector().wakeup(); // <2> finally always 실행
                                }
                            }
                        });

                        // <3> 명시적 콜백
                        workerPool.submit(() -> {
                            System.out.println("[<3>] 명시적 콜백 대기 시작");  // ⭐
                            try {
                                String result = future.get();
                                System.out.println("[<4>] Callback 처리 결과: " + result);  // ⭐
                            } catch (Exception e) {
                                System.err.println("[Callback Error] " + e.getMessage());
                            }
                        });
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("[ERROR] 서버 오류: " + e.getMessage());
        }
    }

    private static void onLoad() {
        try (BufferedReader reader = new BufferedReader(new FileReader(NICKNAMEPATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                allUsers.add(line.trim());
            }
            System.out.println("Loaded " + allUsers.size() + " historical nicknames");
        } catch (Exception e) {
            System.err.println("[onLoad ERROR] " + e.getMessage());
        }
    }

    private static void handleAccept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = server.accept();

        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            clientChannel.register(selector, SelectionKey.OP_READ, new ClientInfo(BUFFER_SIZE));
            System.out.println("[ACCEPT] 클라이언트 연결: " + clientChannel.getRemoteAddress());
        }
    }

    private static String handleReadWrite(SelectionKey key) throws IOException, InterruptedException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ClientInfo clientInfo = (ClientInfo) key.attachment();
        ByteBuffer buffer = clientInfo.getBuffer();

        long threadId = Thread.currentThread().getId();
        int threadNum = threadIdMap.computeIfAbsent(threadId, id -> threadCounter.getAndIncrement());

        int bytesRead = clientChannel.read(buffer);
        if (bytesRead <= 0) return "읽을 데이터 없음";

        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String message = new String(bytes, StandardCharsets.UTF_8).trim();

        if (!clientInfo.isRegistered()) {
            if (activeUsers.contains(message)) {
                clientChannel.write(ByteBuffer.wrap("[SERVER] 중복 닉네임\n".getBytes()));
                buffer.clear();
                return "중복 닉네임 거부됨";
            }

            clientInfo.setNickname(message);
            clientInfo.setRegistered(true);
            activeUsers.add(message);

            if (!allUsers.contains(message)) {
                allUsers.add(message);
                nicknameOutputStream.write((message + "\n").getBytes(StandardCharsets.UTF_8));
                nicknameOutputStream.flush();
            }
            clientChannel.write(ByteBuffer.wrap(("[SERVER] 닉네임 등록 완료: " + message + "\n").getBytes()));
            buffer.clear();
            return "신규 닉네임 등록: " + message;
        }

        String nickname = clientInfo.getNickname();
        String timestamp = "[" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "]";
        String fullMessage = nickname + ": " + message + " " + timestamp + "\n";

        chatOutputStream.write(fullMessage.getBytes(StandardCharsets.UTF_8));
        chatOutputStream.flush();
        buffer.clear();
        return "메시지 수신 및 저장 완료: " + fullMessage.trim();
    }
}