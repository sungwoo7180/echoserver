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

public class ChatServer2 {
    private static final int PORT = 12345;
    private static final int POOL_SIZE = 10;
    private static final int BUFFER_SIZE = 1024;

    private static final String TEXTPATH = "src/test/java/echoserver/main/text/chathistory.txt";
    private static final String NICKNAMEPATH = "src/test/java/echoserver/main/text/nickname.txt";

    private static final ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
    private static final AtomicInteger threadCounter = new AtomicInteger(1);
    private static final Map<Long, Integer> threadIdMap = new ConcurrentHashMap<>();
    private static final Set<String> allUsers = new ConcurrentSkipListSet<>();
    private static final Set<String> activeUsers = new ConcurrentSkipListSet<>();
    private static FileOutputStream nicknameOutputStream;
    private static FileOutputStream chatOutputStream;

    public static void main(String[] args) {
        try {
            createFileIfNotExists(NICKNAMEPATH);
            createFileIfNotExists(TEXTPATH);

            nicknameOutputStream = new FileOutputStream(NICKNAMEPATH, true);
            chatOutputStream = new FileOutputStream(TEXTPATH, true);
            onLoad();

            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(PORT));
            serverChannel.configureBlocking(false);

            Selector selector = Selector.open();
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            Thread selectorThread = new Thread(new SelectorThread(selector, workerPool));
            selectorThread.start();

            System.out.println("Chatting server started on port :" + PORT);
        } catch (IOException e) {
            System.err.println("I/O Error: " + e.getMessage());
        }
    }

    private static void createFileIfNotExists(String path) {
        File file = new File(path);
        if (!file.exists()) {
            try {
                File parentDir = file.getParentFile();
                if (parentDir != null && !parentDir.exists()) {
                    parentDir.mkdirs();
                }
                file.createNewFile();
                System.out.println("[INFO] 새 파일 생성됨: " + path);
            } catch (IOException e) {
                System.err.println("[ERROR] 파일 생성 실패: " + path + " (" + e.getMessage() + ")");
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

    public static void handleAccept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = server.accept();
        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            ClientInfo clientInfo = new ClientInfo(BUFFER_SIZE);
            clientChannel.register(selector, SelectionKey.OP_READ, clientInfo);
            System.out.println("[ACCEPT] 클라이언트 연결 수락: " + clientChannel.getRemoteAddress());
        }
    }

    public static void handleReadWrite(SelectionKey key) {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ClientInfo clientInfo = (ClientInfo) key.attachment();
        ByteBuffer buffer = clientInfo.getBuffer();

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
                String receivedData = new String(bytes, StandardCharsets.UTF_8);

                String[] messages = receivedData.split("\n");
                for (String rawMsg : messages) {
                    String message = rawMsg.trim();
                    if (message.isEmpty()) continue;

                    if (!clientInfo.isRegistered()) {
                        if (activeUsers.contains(message)) {
                            clientChannel.write(ByteBuffer.wrap("[SERVER] 중복된 닉네임입니다.\n".getBytes()));
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
                            while (!history.isEmpty()) {
                                String line = history.pop();
                                clientChannel.write(ByteBuffer.wrap((line + "\n").getBytes()));
                            }
                        }

                        System.out.println("[닉네임 등록] " + message);
                        clientChannel.write(ByteBuffer.wrap(("[SERVER] '" + message + "' 닉네임 등록 완료!\n").getBytes()));
                    } else {
                        String nickname = clientInfo.getNickname();
                        String timestamp = "[" + java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "]";
                        String fullMessage = nickname + ": " + message + " " + timestamp + "\n";

                        chatOutputStream.write(fullMessage.getBytes(StandardCharsets.UTF_8));
                        chatOutputStream.flush();
                        System.out.println("[브로드캐스트] " + fullMessage.trim());

                        broadcastToAllClients(key.selector(), fullMessage);
                    }
                }

                buffer.clear();
            } catch (IOException e) {
                cleanupKey(key);
            }
        }
    }

    private static void broadcastToAllClients(Selector selector, String message) {
        ByteBuffer broadcastBuffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));

        for (SelectionKey key : selector.keys()) {
            if (key.channel() instanceof SocketChannel && key.isValid()) {
                try {
                    SocketChannel sc = (SocketChannel) key.channel();
                    broadcastBuffer.rewind();
                    sc.write(broadcastBuffer);
                } catch (IOException e) {
                    System.err.println("[BROADCAST ERROR] 클라이언트 전송 실패");
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

            System.out.println("[CLEANUP] 연결 종료: " + channel.getRemoteAddress());
            key.cancel();
            channel.close();
        } catch (IOException ignore) {}
    }

    public static Stack<String> readChatHistoryReversed(String filePath, int numberOfLines) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        long pointer = raf.length() - 1;
        Stack<String> stack = new Stack<>();
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