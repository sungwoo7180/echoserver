package echoserver;

import java.io.IOException;
// import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class NioEchoServer2 {
    private static final int PORT = 12346; // 서버가 사용할 포트 번호
    private static final int BUFFER_SIZE = 10000000; // 버퍼 크기 설정
    private static final int WORKER_THREADS = 10; // 작업자(Worker) 스레드 개수 ( 2개 Read, 2개 Write )
    private static final int READ_THREADS = 5;  // Read 전용 스레드 수
    private static final int WRITE_THREADS = 5; // Write 전용 스레드 수
    // private static final int MAX_BYTES = 256; // 메시지 최대 크기 (256바이트 제한)

    // Read 와 Write 작업을 위한 큐 (멀티스레드 환경에서 동기화된 큐 사용) : read/write 별도로 분리하면 작업 유형에 따라 스레드를 전문화 가능
    private static final LinkedBlockingQueue<SelectionKey> readQueue = new LinkedBlockingQueue<>(1000);
    private static final LinkedBlockingQueue<SelectionKey> writeQueue = new LinkedBlockingQueue<>(1000);
    private static final AtomicInteger threadCounter = new AtomicInteger(1); // 쓰레드 번호 할당기 (for 디버깅)

    public static void main(String[] args) {
        ExecutorService workerPool = Executors.newFixedThreadPool(WORKER_THREADS); // Worker Thread Pool 생성
        // ExecutorService workerPool = Executors.newCachedThreadPool();
//        // Read 와 Write 를 담당하는 Worker Thread 실행
//        for (int i = 0; i < WORKER_THREADS; i++) {
//            int threadId = threadCounter.getAndIncrement();
//            workerPool.submit(() -> processReadTasks(threadId)); // Read Worker 실행
//            workerPool.submit(() -> processWriteTasks(threadId)); // Write Worker 실행
//        }
        // Read 전용 스레드 실행 ( 1번 2번 3번 4번 5번 )
        for (int i = 0; i < READ_THREADS; i++) {
            int threadId = threadCounter.getAndIncrement();
            workerPool.submit(() -> {
                System.out.println("read Worker " + threadId + " 실행 (Thread ID: " + Thread.currentThread().getId() + ")");
                processReadTasks(threadId);
            });
            /*
            // (2) Runnable 인라인 오버라이딩 (익명 객체)
            workerPool.submit(new Runnable() {
                @Override
                public void run() {
                    processReadTasks(threadId);
                }
            });
            */
            System.out.println("Read Worker " + threadId + " 생성");
        }

        // Write 전용 스레드 실행 ( 6번 7번 8번 9번 10번 )
        for (int i = 0; i < WRITE_THREADS; i++) {
            int threadId = threadCounter.getAndIncrement();
            workerPool.submit(() -> {
                System.out.println("write Worker " + threadId + " 실행 (Thread ID: " + Thread.currentThread().getId() + ")");
                processWriteTasks(threadId);
            });
            System.out.println("Write Worker " + threadId + " 생성");
        }

        try (Selector selector = Selector.open(); // Selector(이벤트 기반 매니저) 생성
             ServerSocketChannel serverSocket = ServerSocketChannel.open()) { // 1. socket() : 서버 소켓 생성

            serverSocket.configureBlocking(false);                      // 논블로킹 모드 설정
            serverSocket.bind(new InetSocketAddress(PORT));             // 2. bind() : 포트 바인딩
            // bind() 를 하는 이유? 서버 소켓이 특정 포트와 인터페이스에 연결되도록 설정하는 과정, bind() 없이 서버를 실행하면,
            // 클라이언트가 서버를 찾을 수 없어서 통신이 불가능함.
            serverSocket.register(selector, SelectionKey.OP_ACCEPT);    // accept 이벤트 감지 등록 -> SelectionKey 생성
            // 3. listen()을 Selector 에게 양도

            System.out.println("서버가 포트 " + PORT + "에서 실행 중입니다...");

            while (true) {
                                    // 이벤트 발생할 때까지 대기 (Blocking 상태) 3. listen()
                selector.select();  // ** 이벤트가 있는 소켓만 선택 **     // selector 에 timeout 을 주는 방법도 있다.

                // SelectionKey 는 채널과 이벤트를 연결하는 객체.
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove(); // 처리된 키는 제거

                    if (!key.isValid()) {       // CancelledKeyException 방지 코드
                        key.cancel();
                        continue;               // 유효하지 않은 키는 무시하고 다시 while 문 처음으로
                    }
                    try {
                        if (key.isAcceptable()) {           // 클라이언트 connect(), 서버가 accept() 가능할때
                            handleAccept(key, selector);    // 클라이언트 연결 처리
                        } else if (key.isReadable()) {      // 데이터가 준비된 소켓만 읽음
                            readQueue.offer(key);           // Read 작업 큐에 추가
                        } else if (key.isWritable()) {
                            writeQueue.offer(key);          // Write 작업 큐에 추가
                        }
                    } catch ( CancelledKeyException e ) {
                        e.printStackTrace();
                        key.cancel();
                        System.err.println("CancelledKeyException 발생: " + e.getMessage());
                    }

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            workerPool.shutdown(); // 스레드 풀 종료
        }
    }

    // 클라이언트의 연결 요청을 처리하는 메서드
    private static void handleAccept(SelectionKey key, Selector selector) throws IOException {

        // *** 현재 SelectionKey 가 관리하는 채널을 가져와 ServerSocketChannel 로 캐스팅 ***
        ServerSocketChannel serverSocket = (ServerSocketChannel) key.channel();

        SocketChannel clientSocket = serverSocket.accept(); // 4. accept() 클라이언트 연결 수락
        clientSocket.configureBlocking(false); // 논블로킹 모드 설정

        // 클라이언트 별 버퍼 할당 (데이터 유지 가능하도록)
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        // 클라이언트 소켓을 Selector 에 등록하여 Read 이벤트를 감지하도록 설정
        clientSocket.register(selector, SelectionKey.OP_READ, buffer);
        System.out.println("클라이언트 연결 수락: " + clientSocket.getRemoteAddress());
    }

    // Read 작업을 수행하는 Worker Thread 실행 (쓰레드 ID 표시)
    private static void processReadTasks(int threadId) {
        while (true) {
            try {
                SelectionKey key = readQueue.take(); // ***Read 작업 큐에서 가져옴***
                // take() 는 블로킹 메소드임, 큐가 비어있으면 작업이 들어올때까지 Thread 가 대기함.
                // 여러 개의 Read Thread 가 있지만, 한 번에 하나의 스레드만 큐에서 key 를 가져가서 처리하게 됩니다.
                // 각각의 작업을 하나의 스레드만 가져갈 수 있도록 동작하므로 Race Condition 은 발생하지 않습니다.
                System.out.println("[Thread " + threadId + "번 - " + Thread.currentThread().getId() + "] read 처리 중...");
                handleRead(key, threadId);
            } catch (InterruptedException | IOException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    //

    // Write 작업을 수행하는 Worker Thread 실행 (쓰레드 ID 표시)
    private static void processWriteTasks(int threadId) {
        while (true) {
            try {
                SelectionKey key = writeQueue.take(); // ***Write 작업 큐에서 가져옴***
                System.out.println("[Thread " + threadId + "번 - " + Thread.currentThread().getId() + "] Write 처리 중...");
                handleWrite(key);
            } catch (InterruptedException | IOException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    // 클라이언트가 보낸 데이터를 읽고 처리하는 메서드
    private static void handleRead(SelectionKey key, int threadId) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        synchronized (key) { // 여러 Read Thread 가 동시에 접근하지 못하도록 동기화
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) {  //
                System.out.println("클라이언트 연결 종료: " + channel.getRemoteAddress());
                key.cancel();
                channel.close();
                return;
            }

            buffer.flip();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            String message = new String(bytes).trim();

            if (!message.isEmpty()) {
                System.out.println("[Thread " + threadId + "번 - " + Thread.currentThread().getId() + "] 클라이언트(" + channel.getRemoteAddress() + ") 메시지: " + message);

                // **Write Queue 에 단 한 번만 추가**
                ByteBuffer responseBuffer = ByteBuffer.wrap(("Echo: " + message).getBytes());
                key.attach(responseBuffer);

                key.interestOps(SelectionKey.OP_WRITE);
                key.selector().wakeup();

                synchronized (writeQueue) { // WriteQueue 도 동기화
                    if (!writeQueue.contains(key)) {
                        writeQueue.offer(key); // 중복 추가 방지
                    }
                }
            }
            buffer.clear();
            key.interestOps(SelectionKey.OP_READ);
        }
    }

    // 클라이언트에게 데이터를 보내는 메서드
    private static void handleWrite(SelectionKey key) throws IOException {

        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        synchronized (key) { // 여러 Write Thread 가 동시에 접근하지 못하도록 동기화
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            buffer.clear();

            // 다시 Read 상태로 변경
            key.interestOps(SelectionKey.OP_READ);
            key.selector().wakeup();
        }
    }
    // read 함수 원형의 주석달기
    /*
    public int read(ByteBuffer buf) throws IOException {
        Objects.requireNonNull(buf); // 널 체크 (buf 가 null 이면 NullPointerException 발생)

        readLock.lock(); // 여러 스레드가 동시에 읽지 못하도록 락 걸기
        try {
            ensureOpenAndConnected(); // 채널이 열려있고 연결되어 있는지 확인
            boolean blocking = isBlocking(); // 현재 채널이 블로킹 모드인지 확인
            int n = 0; // 읽은 바이트 수 저장 변수

            try {
                beginRead(blocking); // 읽기 작업 시작 (JVM 내부적으로 I/O 상태 관리)

                // 클라이언트가 연결을 강제로 종료했는지 확인
                if (connectionReset)
                    throwConnectionReset();

                // 입력 스트림이 닫혔는지 확인
                if (isInputClosed)
                    return IOStatus.EOF; // -1 반환 (EOF 상태)

                // 실제 데이터를 읽어들임 (fd: 소켓 파일 디스크립터)
                n = IOUtil.read(fd, buf, -1, nd);

                // 블로킹 모드일 때 추가적인 처리 (논블로킹 모드에서는 실행 안 됨)
                if (blocking) {
                    while (IOStatus.okayToRetry(n) && isOpen()) {
                        park(Net.POLLIN); // 블로킹 상태에서 읽을 데이터가 있을 때까지 대기
                        n = IOUtil.read(fd, buf, -1, nd); // 다시 읽기 시도
                    }
                }
            } catch (ConnectionResetException e) {
                connectionReset = true;
                throwConnectionReset(); // 연결이 강제 종료되었을 경우 예외 처리
            } finally {
                endRead(blocking, n > 0); // 읽기 작업 종료 처리
                if (n <= 0 && isInputClosed)
                    return IOStatus.EOF; // -1 반환
            }
            return IOStatus.normalize(n); // 읽은 바이트 수 반환 (정상화된 값)
        } finally {
            readLock.unlock(); // 락 해제
        }
    }
    */
}
