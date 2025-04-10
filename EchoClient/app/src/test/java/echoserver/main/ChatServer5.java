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

public class ChatServer5 {
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

    public static void main(String[] args) {
        try (
                ServerSocketChannel serverChannel = ServerSocketChannel.open();
                Selector selector = Selector.open();
        ) {
            nicknameOutputStream = new FileOutputStream(NICKNAMEPATH, true);
            chatOutputStream = new FileOutputStream(TEXTPATH, true);
            onLoad();

            serverChannel.bind(new InetSocketAddress(PORT));
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("ChatServer5 started on port " + PORT);

            while (true) {
                selector.select();
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();

                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();

                    if (!key.isValid()) continue;

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
                                    key.selector().wakeup();
                                }
                            }
                        });
                    }
                }
            }

        } catch (IOException e) {
            System.err.println("서버 오류: " + e.getMessage());
        } finally {
            workerPool.shutdown();
            try {
                if (nicknameOutputStream != null) nicknameOutputStream.close();
                if (chatOutputStream != null) chatOutputStream.close();
            } catch (IOException ignore) {}
        }
    }

    private static void onLoad() {
        try (BufferedReader reader = new BufferedReader(new FileReader(NICKNAMEPATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                allUsers.add(line.trim());
            }
        } catch (IOException e) {
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
            System.out.println("[ACCEPT] 클라이언트 접속: " + clientChannel.getRemoteAddress());
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
                int bytesRead = clientChannel.read(buffer);
                if (bytesRead == -1) {
                    cleanupKey(key);
                    return;
                } else if (bytesRead == 0) {
                    return;
                }

                buffer.flip();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                String[] messages = new String(bytes, StandardCharsets.UTF_8).split("\n");

                for (String msg : messages) {
                    String message = msg.trim();
                    if (message.isEmpty()) continue;

                    if (!clientInfo.isRegistered()) {
                        if (activeUsers.contains(message)) {
                            clientChannel.write(ByteBuffer.wrap("[SERVER] 중복된 닉네임입니다. 3초 후 연결 종료.\n".getBytes()));
                            clientChannel.socket().shutdownOutput();
                            Thread.sleep(3000);
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
                        } else {
                            Stack<String> history = readChatHistoryReversed(TEXTPATH, 20);
                            for (String line : history) {
                                clientChannel.write(ByteBuffer.wrap((line + "\n").getBytes()));
                            }
                        }
                        clientChannel.write(ByteBuffer.wrap(("[SERVER] '" + message + "' 닉네임 등록 완료!\n").getBytes()));
                    } else {
                        if (handleCommand(message, clientInfo, key.selector())) return;

                        String nickname = clientInfo.getNickname();
                        String timestamp = "[" + java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "]";
                        String fullMessage = nickname + ": " + message + " " + timestamp + "\n";

                        chatOutputStream.write(fullMessage.getBytes(StandardCharsets.UTF_8));
                        chatOutputStream.flush();
                        broadcastToAllClients(key.selector(), fullMessage);
                    }
                }

                buffer.clear();
            } catch (Exception e) {
                cleanupKey(key);
            }
        }
    }

    private static boolean handleCommand(String message, ClientInfo clientInfo, Selector selector) throws IOException {
        String nickname = clientInfo.getNickname();
        if ("/shutdown".equalsIgnoreCase(message) && "ADMIN".equalsIgnoreCase(nickname)) {
            broadcastToAllClients(selector, "[SERVER] 서버가 관리자에 의해 종료됩니다.\n");
            System.out.println("[COMMAND] 관리자 서버 종료 실행됨");
            System.exit(0);
            return true;
        }
        return false;
    }

    private static void broadcastToAllClients(Selector selector, String message) {
        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));
        for (SelectionKey key : selector.keys()) {
            if (key.channel() instanceof SocketChannel && key.isValid()) {
                try {
                    ((SocketChannel) key.channel()).write(buffer.duplicate());
                } catch (IOException e) {
                    cleanupKey(key);
                }
            }
        }
    }

    private static void cleanupKey(SelectionKey key) {
        try {
            SocketChannel channel = (SocketChannel) key.channel();
            ClientInfo info = (ClientInfo) key.attachment();
            if (info != null && info.getNickname() != null) {
                activeUsers.remove(info.getNickname());
            }
            channel.close();
            key.cancel();
            System.out.println("[CLEANUP] 연결 종료됨");
        } catch (IOException ignore) {}
    }

    private static Stack<String> readChatHistoryReversed(String filePath, int numberOfLines) throws IOException {
        Stack<String> stack = new Stack<>();
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        long pointer = raf.length() - 1;
        ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream();

        while (pointer >= 0 && stack.size() < numberOfLines) {
            raf.seek(pointer);
            byte b = raf.readByte();
            if (b == '\n') {
                byte[] lineBytes = reverseBytes(lineBuffer.toByteArray());
                stack.push(new String(lineBytes, StandardCharsets.UTF_8).trim());
                lineBuffer.reset();
            } else {
                lineBuffer.write(b);
            }
            pointer--;
        }

        if (lineBuffer.size() > 0) {
            byte[] lineBytes = reverseBytes(lineBuffer.toByteArray());
            stack.push(new String(lineBytes, StandardCharsets.UTF_8).trim());
        }

        raf.close();
        return stack;
    }

    private static byte[] reverseBytes(byte[] input) {
        byte[] reversed = new byte[input.length];
        for (int i = 0; i < input.length; i++) {
            reversed[i] = input[input.length - 1 - i];
        }
        return reversed;
    }
}
