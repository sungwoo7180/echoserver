package echoserver.main;

import echoserver.main.domain.ClientInfo;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ChatServer3 {
    private final int PORT;
    private final int POOL_SIZE;
    private final int BUFFER_SIZE;
    private final String TEXTPATH;
    private final String NICKNAMEPATH;

    private final ExecutorService workerPool;
    private final AtomicInteger threadCounter = new AtomicInteger(1);
    private final Map<Long, Integer> threadIdMap = new ConcurrentHashMap<>();
    private final Set<String> allUsers = new ConcurrentSkipListSet<>();
    private final Set<String> activeUsers = new ConcurrentSkipListSet<>();
    private FileOutputStream nicknameOutputStream;
    private FileOutputStream chatOutputStream;

    public ChatServer3(int port, int poolSize, int bufferSize, String textPath, String nicknamePath) {
        this.PORT = port;
        this.POOL_SIZE = poolSize;
        this.BUFFER_SIZE = bufferSize;
        this.TEXTPATH = textPath;
        this.NICKNAMEPATH = nicknamePath;
        this.workerPool = Executors.newFixedThreadPool(POOL_SIZE);
    }

    @Test
    public void run() {
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

            System.out.println("Chatting server started on port :" + PORT);

            while (!Thread.currentThread().isInterrupted()) {
                selector.select();
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();

                    if (!key.isValid() || !key.channel().isOpen()) {
                        key.cancel();
                        continue;
                    }

                    if (key.isAcceptable()) {
                        handleAccept(key, selector);
                    } else if (key.isReadable()) {
                        try {
                            int testRead = ((SocketChannel) key.channel()).read(ByteBuffer.allocate(1));
                            if (testRead == -1) {
                                System.out.println("중앙 루프에서 종료 감지: " + key.channel());
                                key.cancel();
                                key.channel().close();
                                continue;
                            }
                            workerPool.submit(() -> handleReadWrite(key));
                        } catch (IOException e) {
                            System.err.println("[중앙 루프] 예외 발생: " + e.getMessage());
                            try {
                                key.cancel();
                                key.channel().close();
                            } catch (IOException ex) {
                                System.err.println("[중앙 루프] 종료 중 예외: " + ex.getMessage());
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("I/O Error: " + e.getMessage());
        } finally {
            workerPool.shutdown();
        }
    }

    @Test
    public void onLoad() {
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

    public void handleAccept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = server.accept();

        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            ClientInfo clientInfo = new ClientInfo(BUFFER_SIZE);
            clientChannel.register(selector, SelectionKey.OP_READ, clientInfo);
            System.out.println("[ACCEPT] 클라이언트 연결 수락: " + clientChannel.getRemoteAddress());
        }
    }

    public void handleReadWrite(SelectionKey key) {
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
                            Stack<String> history = readChatHistoryReversed(TEXTPATH, 20);
                            while (!history.isEmpty()) {
                                String line = history.pop();
                                clientChannel.write(ByteBuffer.wrap((line + "\n").getBytes()));
                            }
                            System.out.println("[Thread " + threadNum + "] 기존 유저 '" + message + "' 히스토리 전송");
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
                buffer.clear();
            } catch (IOException e) {
                System.err.println("[Thread " + threadNum + "] [ERROR] 읽기 중 예외 발생");
                if (key.isValid()) {
                    cleanupKey(key);
                }
            }
        }
    }

    public void broadcastToAllClients(Selector selector, String message) {
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

    public void cleanupKey(SelectionKey key) {
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

    public Stack<String> readChatHistoryReversed(String filePath, int numberOfLines) throws IOException {
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

    public byte[] reverseBytes(byte[] input) {
        byte[] reversed = new byte[input.length];
        for (int i = 0; i < input.length; i++) {
            reversed[i] = input[input.length - 1 - i];
        }
        return reversed;
    }
}
