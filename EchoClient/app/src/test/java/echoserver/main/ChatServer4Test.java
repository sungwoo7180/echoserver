package echoserver.main;

import echoserver.main.domain.ClientInfo;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ChatServer4Test {
    private static final int PORT = 12345;
    private static final int BUFFER_SIZE = 1024;

    private static final String TEXTPATH = "app/src/test/java/echoserver/main/text/chathistory.txt";
    private static final String NICKNAMEPATH = "app/src/test/java/echoserver/main/text/nickname.txt";

    private static final ExecutorService workerPool = Executors.newCachedThreadPool();
    private static final AtomicInteger threadCounter = new AtomicInteger(1);
    private static final Map<Long, Integer> threadIdMap = new ConcurrentHashMap<>();
    private static final Set<String> allUsers = new ConcurrentSkipListSet<>();
    private static final Set<String> activeUsers = new ConcurrentSkipListSet<>();
    private static FileOutputStream nicknameOutputStream;
    private static FileOutputStream chatOutputStream;

    private static final AtomicInteger broadcastCount = new AtomicInteger();

    public static void main(String[] args) {
        try (
                ServerSocketChannel serverChannel = ServerSocketChannel.open();
                Selector selector = Selector.open();
        ) {
            nicknameOutputStream = new FileOutputStream(NICKNAMEPATH, true);
            chatOutputStream = new FileOutputStream(TEXTPATH, true);
            onLoad();

            serverChannel.bind(new InetSocketAddress(PORT), 400);
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("Chatting server started on port :" + PORT);

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
                        workerPool.submit(() -> {
                            try {
                                handleReadWrite(key);
                            } finally {
                                if (key.isValid()) {
                                    key.interestOps(SelectionKey.OP_READ);
                                }
                            }
                        });
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("I/O Error: " + e.getMessage());
        } finally {
            workerPool.shutdown();
            try {
                if (nicknameOutputStream != null) nicknameOutputStream.close();
                if (chatOutputStream != null) chatOutputStream.close();
            } catch (IOException e) {
                System.err.println("[ERROR] 파일 스트림 닫기 실패: " + e.getMessage());
            }
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
            System.err.println("[onLoad ERROR] 닉네임 로드 실패: " + e.getMessage());
        }
    }

    private static void handleAccept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = server.accept();

        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            ClientInfo clientInfo = new ClientInfo(BUFFER_SIZE);
            clientChannel.register(selector, SelectionKey.OP_READ, clientInfo);
            System.out.println("[ACCEPT] 클라이언트 연결 수락: " + clientChannel.getRemoteAddress());
        }
    }

    private static void handleReadWrite(SelectionKey key) {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ClientInfo clientInfo = (ClientInfo) key.attachment();
        ByteBuffer buffer = clientInfo.getBuffer();

        long threadId = Thread.currentThread().getId();
        int threadNum = threadIdMap.computeIfAbsent(threadId, id -> threadCounter.getAndIncrement());

        synchronized (key) {
            try {
                int bytesRead;
                while ((bytesRead = clientChannel.read(buffer)) > 0) {
                    buffer.flip();
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    buffer.clear();
                    String receivedData = new String(bytes, StandardCharsets.UTF_8);

                    String[] messages = receivedData.split("\n");
                    for (String rawMsg : messages) {
                        String message = rawMsg.trim();
                        if (message.isEmpty()) continue;

                        if (!clientInfo.isRegistered()) {
                            if (activeUsers.contains(message)) {
                                clientChannel.write(ByteBuffer.wrap("[SERVER] 중복된 닉네임입니다.\n".getBytes()));
                                System.out.println("[Thread " + threadNum + "] 중복 닉네임 '" + message + "' 거부");
                                cleanupKey(key);
                                return;
                            }

                            clientInfo.setNickname(message);
                            clientInfo.setRegistered(true);
                            activeUsers.add(message);

                            if (!allUsers.contains(message)) {
                                allUsers.add(message);
                                nicknameOutputStream.write((message + "\n").getBytes(StandardCharsets.UTF_8));
                                nicknameOutputStream.flush();
                                System.out.println("[Thread " + threadNum + "] 신규 유저 '" + message + "' 닉네임 등록");
                            } else {
                                System.out.println("[Thread " + threadNum + "] 기존 유저 '" + message + "' 히스토리 전송 생략");
                            }
                            clientChannel.write(ByteBuffer.wrap(("[SERVER] '" + message + "' 닉네임 등록 완료!\n").getBytes()));
                        } else {
                            String nickname = clientInfo.getNickname();
                            String timestamp = "[" + java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "]";
                            String fullMessage = nickname + ": " + message + " " + timestamp + "\n";

                            chatOutputStream.write(fullMessage.getBytes(StandardCharsets.UTF_8));
                            chatOutputStream.flush();
                            System.out.println("[Thread " + threadNum + "] [브로드캐스트] " + fullMessage.trim());
                            broadcastToAllClients(key.selector(), fullMessage);
                        }
                    }
                }
                if (bytesRead == -1) {
                    System.out.println("[Thread " + threadNum + "] 클라이언트 연결 종료: " + clientChannel.getRemoteAddress());
                    cleanupKey(key);
                }
            } catch (IOException e) {
                cleanupKey(key);
                System.err.println("[Thread " + threadNum + "] [ERROR] 읽기 중 예외 발생");
            }
        }
    }

    private static void broadcastToAllClients(Selector selector, String message) {
        ByteBuffer broadcastBuffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));
        int sentCount = 0;

        for (SelectionKey key : selector.keys()) {
            if (key.channel() instanceof SocketChannel && key.isValid()) {
                try {
                    SocketChannel sc = (SocketChannel) key.channel();
                    broadcastBuffer.rewind();
                    sc.write(broadcastBuffer);
                    sentCount++;
                } catch (IOException e) {
                    System.err.println("[BROADCAST ERROR] 클라이언트 전송 실패");
                    cleanupKey(key);
                }
            }
        }
        broadcastCount.addAndGet(sentCount);
        System.out.println("[BROADCAST] 현재까지 총 전송 수: " + broadcastCount.get());
    }

    private static void cleanupKey(SelectionKey key) {
        try {
            SocketChannel channel = (SocketChannel) key.channel();
            ClientInfo info = (ClientInfo) key.attachment();

            if (info != null && info.getNickname() != null) {
                activeUsers.remove(info.getNickname());
            }

            System.out.println("[CLEANUP] 연결 종료: " + channel.getRemoteAddress());
            key.cancel();
            channel.close();
        } catch (IOException ignore) {}
    }

    private static byte[] reverseBytes(byte[] input) {
        byte[] reversed = new byte[input.length];
        for (int i = 0; i < input.length; i++) {
            reversed[i] = input[input.length - 1 - i];
        }
        return reversed;
    }
}