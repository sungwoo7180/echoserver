package echoserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NioEchoServer {
    private static final int PORT = 12345; // 서버가 사용할 포트 번호
    private static final int BUFFER_SIZE = 1024; // 버퍼 크기 설정
    private static final int WORKER_THREADS = 3; // 작업자(Worker) 스레드 개수

    public static void main(String[] args) {
        // 작업자 스레드 풀 (Worker Thread Pool)
        ExecutorService workerPool = Executors.newFixedThreadPool(WORKER_THREADS);

        try (Selector selector = Selector.open(); // Selector(이벤트 기반 매니저) 생성
             ServerSocketChannel serverSocket = ServerSocketChannel.open()) {

            // 서버 소켓 채널을 논블로킹 모드로 설정
            serverSocket.configureBlocking(false);
            // 지정된 포트에 바인딩하여 클라이언트의 연결을 대기
            serverSocket.bind(new InetSocketAddress(PORT));
            // 서버 소켓을 Selector에 등록하여 ACCEPT 이벤트 감지 ( 클라이언트의 연결을 감지 )
            serverSocket.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("서버가 포트 " + PORT + "에서 실행 중입니다...");

            while (true) {
                // Selector가 감지할 때까지 대기 (Non-blocking)
                selector.select();

                // 감지된 이벤트 목록을 가져옴
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove(); // 현재 이벤트 처리 후 제거

                    if (key.isAcceptable()) {
                        // Accept 이벤트 발생 (클라이언트 연결 요청)
                        handleAccept(key, selector);
                    } else if (key.isReadable()) {
                        // Read 이벤트 발생 (클라이언트가 데이터를 보냄)
                        key.interestOps(0); // 추가 이벤트 감지를 방지
                        workerPool.submit(() -> handleRead(key));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            workerPool.shutdown(); // 스레드 풀 종료
        }
    }

    // 클라이언트 연결 요청을 처리하는 메서드 (Accept 전담)
    private static void handleAccept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel serverSocket = (ServerSocketChannel) key.channel();
        // 클라이언트 연결 수락
        SocketChannel clientSocket = serverSocket.accept();
        clientSocket.configureBlocking(false); // 논블로킹 모드 설정
        // 클라이언트 소켓을 Selector에 등록 (Read 이벤트 감지)
        clientSocket.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(BUFFER_SIZE));
        System.out.println("클라이언트 연결 수락: " + clientSocket.getRemoteAddress());
    }

    // 클라이언트로부터 데이터를 읽는 메서드 (Worker 스레드에서 실행됨)
    private static void handleRead(SelectionKey key) {
        SocketChannel clientSocket = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment();

        try {
            int bytesRead = clientSocket.read(buffer);
            if (bytesRead == -1) {
                // 클라이언트가 연결을 종료한 경우
                clientSocket.close();
                System.out.println("클라이언트 연결 종료");
                return;
            }

            buffer.flip(); // 읽기 모드로 변경
            while (buffer.hasRemaining()) {
                clientSocket.write(buffer); // 에코 메시지 전송
                System.out.println("클라이언트로부터 받은 메시지: " + new String(buffer.array()));
            }
            buffer.clear(); // 버퍼 초기화

            key.interestOps(SelectionKey.OP_READ); // 다시 Read 이벤트 감지
            key.selector().wakeup(); // Selector를 다시 동작하도록 깨움
        } catch (IOException e) {
            e.printStackTrace();
            try {
                clientSocket.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
