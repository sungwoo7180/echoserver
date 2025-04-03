package echoserver.main;

import echoserver.main.domain.ClientInfo;

import java.beans.Transient;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ChatServer {
    private static final int PORT = 12345;
    private static final int POOL_SIZE = 10;
    private static final int BUFFER_SIZE = 1024;

    private static final String TEXTPATH = "app/src/test/java/echoserver/main/text/chathistory.txt"; // TODO 수정 필요
    private static final String NICKNAMEPATH = "app/src/test/java/echoserver/main/text/nickname.txt"; // TODO 수정 필요

    private static final ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
    private static final AtomicInteger threadCounter = new AtomicInteger(1);    // 스레드 번호 부여기
    private static final Map<Long, Integer> threadIdMap = new ConcurrentHashMap<>();     // 실제 Thread ID -> 넘버링
    private static final Set<String> nicknames = new ConcurrentSkipListSet<>();

    private static FileOutputStream nicknameOutputStream;                                // 닉네임 저장
    private static FileOutputStream chatOutputStream;                                    // 채팅 내역 저장

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

            System.out.println("Chatting server started on port :" + PORT);

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

                    if (key.isAcceptable()) {
                        handleAccept(key, selector);
                    } else if (key.isReadable()) {
                        workerPool.submit(() -> handleReadWrite(key));
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("I/O Error: " + e.getMessage());
        } finally {
            workerPool.shutdown();
            // chatInputStream.close();
        }
    }

    private static void onLoad() {
        // 중복 닉네임 체크를 위해서 Nickname.txt 에서 Set 으로 들고 오기
        try (BufferedReader reader = new BufferedReader(new FileReader(NICKNAMEPATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                nicknames.add(line.trim()); // 공백 제거 후 Set 에 추가
                // System.out.println(line);
            }
            System.out.println("Loaded " + nicknames.size() + " nicknames");
        } catch ( Exception e ) {
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

    private static boolean receiveNickName(SelectionKey key, String nickname) throws IOException {
        // 1. Client(Key 를 통해서 어떤 Client 인지 확인) 에서 받은 닉네임이 기존에 닉네임 중에 있는지 확인하고
        // 2. Set 에 넣어줌.
        // 3. 그리고 바로 Nickname.txt 메모장에도 넣어줌.


        return true;
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
                    System.out.println("[Thread " + threadNum + "] 클라이언트 연결 종료: " + clientChannel.getRemoteAddress());
                    cleanupKey(key);
                    return;
                }

                buffer.flip();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                String message = new String(bytes).trim();

//                if (!message.isEmpty()) {
//                    System.out.println("[Thread " + threadNum + "] 클라이언트(" + clientChannel.getRemoteAddress() + ")메시지: " + message);
//                    ByteBuffer responseBuffer = ByteBuffer.wrap(("Echo: " + message + "\n").getBytes());
//                    while (responseBuffer.hasRemaining()) {
//                        clientChannel.write(responseBuffer);
//                    }
//                    System.out.println("[Thread " + threadNum + "] 클라이언트(" + clientChannel.getRemoteAddress() + ")에게 응답 전송 완료");
//                }
                if (!message.isEmpty()) {
                    if (!clientInfo.isRegistered()) {
                        if (!nicknames.contains(message)) {
                            clientInfo.setNickname(message);
                            clientInfo.setRegistered(true);
                            nicknames.add(message);
                            nicknameOutputStream.write((message + "\n").getBytes(StandardCharsets.UTF_8));
                            nicknameOutputStream.flush();
                            System.out.println("[Thread " + threadNum + "] 닉네임 등록: " + message);
                        } else {
                            clientChannel.write(ByteBuffer.wrap("[SERVER] 중복된 닉네임입니다.\n".getBytes()));
                            cleanupKey(key);
                            return;
                        }
                    } else {
                        System.out.println("[Thread " + threadNum + "] (" + clientInfo.getNickname() + ") 메시지: " + message);
                        ByteBuffer responseBuffer = ByteBuffer.wrap((clientInfo.getNickname() + ": " + message + "\n").getBytes());
                        while (responseBuffer.hasRemaining()) {
                            clientChannel.write(responseBuffer);
                        }
                    }
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

    // 최신순으로 채팅 내역을 20개 들고오는 method
    public static List<String> readChatHistoryReversed(String filePath, int numberOfLines) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        long pointer = raf.length() - 1;    // 파일 크기 - 1 = 파일
        List<String> result = new ArrayList<>();
        ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream();

        while (pointer >= 0 && result.size() < numberOfLines) {
            raf.seek(pointer);
            byte b = raf.readByte();

            // 라인 종료 문자(\n) r확인
            if (b == '\n') {
                // 현재까지 모은 바이트는 거꾸로 되어 있으므로 뒤집기
                byte[] lineBytes = reverseBytes(lineBuffer.toByteArray());
                result.add(new String(lineBytes, StandardCharsets.UTF_8).trim());
                lineBuffer.reset();  // 다음 줄을 위해 초기화
            } else {
                lineBuffer.write(b);  // 거꾸로 읽기
            }
            pointer--;
        }

        // 파일 시작이면서 아직 라인이 남아있을 수 있으니 마지막 줄 처리
        if (lineBuffer.size() > 0) {
            byte[] lineBytes = reverseBytes(lineBuffer.toByteArray());
            String line = new String(lineBytes, StandardCharsets.UTF_8);
            result.add(line.trim());
        }

        raf.close();
        return result;
    }

    private static byte[] reverseBytes(byte[] input) {
        byte[] reversed = new byte[input.length];
        for (int i = 0; i < input.length; i++) {
            reversed[i] = input[input.length - 1 - i];
        }
        return reversed;
    }
}
