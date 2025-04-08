package echoserver.main;

import echoserver.main.domain.ClientInfo;
import org.openjdk.jmh.annotations.*;

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

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime) // 벤치마크 대상 메소드를 실행하는 데 걸린 평균 시간 측정
@OutputTimeUnit(TimeUnit.MILLISECONDS) // 벤치마크 결과를 밀리초 단위로 출력
@Fork(value = 2, jvmArgs = {"-Xms4G", "-Xmx4G"}) // 4Gb의 힙 공간을 제공한 환경에서 두 번 벤치마크를 수행해 결과의 신뢰성 확보
public class ChatServer4 {
    private static final int PORT = 12345;
    private static final int POOL_SIZE = 10;
    private static final int BUFFER_SIZE = 1024;

    private static final String TEXTPATH = "app/src/test/java/echoserver/main/text/chathistory.txt";
    private static final String NICKNAMEPATH = "app/src/test/java/echoserver/main/text/nickname.txt";

    private static final ExecutorService workerPool = Executors.newCachedThreadPool();
    private static final AtomicInteger threadCounter = new AtomicInteger(1);    // 스레드 번호 부여기
    private static final Map<Long, Integer> threadIdMap = new ConcurrentHashMap<>();     // 실제 Thread ID -> 넘버링
    private static final Set<String> allUsers = new ConcurrentSkipListSet<>();
    private static final Set<String> activeUsers = new ConcurrentSkipListSet<>();
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

            serverChannel.bind(new InetSocketAddress(PORT), 200);
            serverChannel.configureBlocking(false);                     // Non-blocking Mode 로 설정
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("Chatting server started on port :" + PORT);

            while (true) {
                selector.select();                                      // I/O 이벤트 대기, 선택된 Key 가 없으면 무한히 대기.
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
                        key.interestOps(0);
                        workerPool.submit(() -> {
                            try {
                                handleReadWrite(key);
                            } finally {
                                if (key.isValid()) {
                                    key.interestOps(SelectionKey.OP_READ);
                                }
                            }
                        });                    }
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


    // 서버 시작 시 nickname.txt -> allUsers 에 로딩
    private static void onLoad() {
        // 중복 닉네임 체크를 위해서 Nickname.txt 에서 Set 으로 들고 오기
        try (BufferedReader reader = new BufferedReader(new FileReader(NICKNAMEPATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                allUsers.add(line.trim()); // 공백 제거 후 Set 에 추가
                // System.out.println(line);
            }
            System.out.println("Loaded " + allUsers.size() + " historical nicknames");
        } catch ( Exception e ) {
            System.err.println("[onLoad ERROR] 닉네임 로드 실패: " + e.getMessage());
        }
    }

    private static void handleAccept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = server.accept();

        if (clientChannel != null) {
            clientChannel.configureBlocking(false);                 // Non-blocking 모드로 설정. true 이면 register 에서 오류뜸.
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
                int bytesRead = clientChannel.read(buffer);     // 읽을 수 없으면 0을 반환
                if (bytesRead == -1) {
                    System.out.println("[Thread " + threadNum + "] 클라이언interestOps트 연결 종료: " + clientChannel.getRemoteAddress());
                    cleanupKey(key);
                    return;
                } else if (bytesRead == 0) {
                    Thread.sleep(1);
                    return;
                }

                buffer.flip();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                String receivedData = new String(bytes, StandardCharsets.UTF_8);

//                if (!message.isEmpty()) {
//                    System.out.println("[Thread " + threadNum + "] 클라이언트(" + clientChannel.getRemoteAddress() + ")메시지: " + message);
//                    ByteBuffer responseBuffer = ByteBuffer.wrap(("Echo: " + message + "\n").getBytes());
//                    while (responseBuffer.hasRemaining()) {
//                        clientChannel.write(responseBuffer);
//                    }
//                    System.out.println("[Thread " + threadNum + "] 클라이언트(" + clientChannel.getRemoteAddress() + ")에게 응답 전송 완료");
//                }
                String[] messages = receivedData.split("\n");
                for (String rawMsg : messages) {
                    String message = rawMsg.trim();
                    if (message.isEmpty()) continue;

                    // 1. 닉네임 등록 처리
                    if (!clientInfo.isRegistered()) {           // 처음 접속 인지?
                        if (activeUsers.contains(message)) {
                            clientChannel.write(ByteBuffer.wrap("[SERVER] 중복된 닉네임입니다.\n".getBytes()));
                            System.out.println("[Thread " + threadNum + "] 중복 닉네임 '" + message + "' 거부");
                            cleanupKey(key);
                            return;
                        }

                        clientInfo.setNickname(message);
                        clientInfo.setRegistered(true);
                        activeUsers.add(message);

                        // 신규 유저는 history X
                        if (!allUsers.contains(message)) {
                            allUsers.add(message);
                            nicknameOutputStream.write((message + "\n").getBytes(StandardCharsets.UTF_8));
                            nicknameOutputStream.flush();
                            System.out.println("[Thread " + threadNum + "] 신규 유저 '" + message + "' 닉네임 등록");
                        } else {
                            // 기존 유저 → history 20줄 전송
                            Stack<String> history = readChatHistoryReversed(TEXTPATH, 20);
                            while (!history.isEmpty()) {                    // 향상된 For 문 쓰면 안됨. List 처럼 똑같이 반환함.
                                String line = history.pop();
                                clientChannel.write(ByteBuffer.wrap((line + "\n").getBytes()));
                            }
                            System.out.println("[Thread " + threadNum + "] 기존 유저 '" + message + "' 히스토리 전송");
                        }
                        clientChannel.write(ByteBuffer.wrap(("[SERVER] '" + message + "' 닉네임 등록 완료!\n").getBytes()));
                    } else { // 2. 일반 메시지 전송
                        String nickname = clientInfo.getNickname();
                        String timestamp = "[" + java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "]";
                        String fullMessage =  nickname + ": " + message + " " + timestamp + "\n";

                        chatOutputStream.write(fullMessage.getBytes(StandardCharsets.UTF_8));
                        chatOutputStream.flush();
                        System.out.println("[Thread " + threadNum + "] [브로드캐스트] " + fullMessage.trim());
                        broadcastToAllClients(key.selector(), fullMessage);
                    }
                }
                buffer.clear();
            } catch (IOException e) {
                cleanupKey(key);
                System.err.println("[Thread " + threadNum + "] [ERROR] 읽기 중 예외 발생");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void broadcastToAllClients(Selector selector, String message) {
        ByteBuffer broadcastBuffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));

        for (SelectionKey key : selector.keys()) {
            if (key.channel() instanceof SocketChannel && key.isValid()) {
                try {
                    SocketChannel sc = (SocketChannel) key.channel();
                    broadcastBuffer.rewind(); // 다시 읽을 수 있도록 rewind
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

    // 최신순으로 채팅 내역을 20개 들고오는 method
    public static Stack<String> readChatHistoryReversed(String filePath, int numberOfLines) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        long pointer = raf.length() - 1;    // raf.length() : 파일의 길이, pointer : 가리키는 index
        Stack<String> stack = new Stack<>();    // List<String> result = new ArrayList<>();
        ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream();

        while (pointer >= 0 && stack.size() < numberOfLines) {
            raf.seek(pointer);
            byte b = raf.readByte();

            // 라인 종료 문자(\n) r확인
            if (b == '\n') {
                // 현재까지 모은 바이트는 거꾸로 되어 있으므로 뒤집기
                byte[] lineBytes = reverseBytes(lineBuffer.toByteArray());
                stack.push(new String(lineBytes, StandardCharsets.UTF_8).trim());   //result.add(new String(lineBytes, StandardCharsets.UTF_8).trim());
                lineBuffer.reset();  // 다음 줄을 위해 초기화
            } else {
                lineBuffer.write(b);  // 거꾸로 읽기
            }
            pointer--;
        }

        // 파일 시작이면서 아직 라인이 남아있을 수 있으니 마지막 줄 처리
        if (lineBuffer.size() > 0) {
            byte[] lineBytes = reverseBytes(lineBuffer.toByteArray());
            stack.push(new String(lineBytes, StandardCharsets.UTF_8).trim());       //result.add(new String(lineBytes, StandardCharsets.UTF_8).trim());
        }

        raf.close();
        // Collections.reverse(stack);   // List 로 받을 경우에 이런식으로 정렬.
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
