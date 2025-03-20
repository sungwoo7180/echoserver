package echoserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EchoThreadServer {
    public static void main(String[] args) {
        // 스레드 풀 생성 (동시 접속 처리용)
        ExecutorService threadPool = Executors.newCachedThreadPool();

        // [1. socket() 2. bind() 3. listen()] : ServerSocket 생성, 12345 포트 바인딩 및 리스닝
        try (ServerSocket server = new ServerSocket(12345)) {
            // 생성자의 구조를 직접 확인하면
            //         this(port, 50, null);
            // 50은 Backlog queue(OS의 TCP 연결 대기큐)의 크기를 의미하며, 클라이언트의 연결 요청을 대기시킬 수 있는 큐의 최대 길이.
            // 51번째 client 요청은 누실 될 수 있다.
            System.out.println("Server is running...");

            // 클라이언트 연결 무한 대기
            while (true) {
                // 4. accept() : 클라이언트 연결 수락 (Blocking I/O)
                Socket socket = server.accept();
                // 동시에 여러 클라이언트가 접속하며 어떻게 될까?
                // A) accept() 는 하나의 클라이언트만 처리할 수 있음. 새로운 스레드가 생성되기 전까지 다른 클라이언트는 대기
                // ( 해당 요청이 처리되기 전에 들어온 요청은 OS의 TCP 연결 대기 큐(Backlog Queue)에 저장됨.)
                // 하지만 accept() 자체가 처리시간이 굉장히 빠름. 대부분의 경우에는 문제가 되지 않음. (클라이언트에서 타임아웃 설정 가능)
                System.out.println("Client connected from " + socket.getRemoteSocketAddress());

                // 스레드 풀에 작업 제출 (각 클라이언트 별 독립 스레드)

                // (2) Runnable 인라인 오버라이딩 (익명 객체)
                threadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        handleClient(socket);
                    }
                });

                // (3) Runnable lambda 식 사용
                //  스레드 풀에 제출  accept()를 통해서 생성된 서버의 client 소켓을 매개변수로 스레드 풀한테 handleClient() 하라고 제출
                // threadPool.submit(() -> { handleClient(socket) });

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            threadPool.shutdown(); // 스레드 풀 종료
            // 8. close(server) : 자동으로 close() 호출
        }
    }

    // 5. request_handler() : accept()로 획득한 소켓에 대해 클라이언트 요청을 처리하는 메서드
    private static void handleClient(Socket socket) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        String clientId = socket.getRemoteSocketAddress().toString();

        // 자원 자동 해제를 위한 try-with-resources
        // finally 에서 명시적으로 닫던 방식을 개선 ( 컴파일러가 자동으로 최적화된 finally 블록 생성 )
        // writer.close() -> reader.close()-> socket.close() 순으로 자동 호출
        try (socket; // Java 9+ : Socket 을 자동으로 close()
             // 입력 스트림: 바이트 → 문자 → 버퍼링 (효율적 읽기)
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(socket.getInputStream()));
             // 출력 스트림: autoFlush=true (println 시 즉시 전송)
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            String line;
            System.out.printf("[%s] %s Connected\n", sdf.format(new Date()), clientId);
            // 클라이언트 데이터 읽기 (readLine 은 개행 기준)
            while ((line = reader.readLine()) != null) { // 클라이언트 연결 종료 시 null 반환
                System.out.printf("[%s] From %s: %s\n", sdf.format(new Date()), clientId, line);

                if ("quit".equals(line)) {
                    writer.println("Server closing connection");
                    break;
                }

                // 6. send_data() : 에코 응답 전송
                writer.println("ECHO: " + line);
            }
            System.out.println("Client disconnected: " + socket.getRemoteSocketAddress());
            // 7. close(client) : 자동으로 close() 호출
        } catch (IOException e) {
            System.err.println("Client handling error: " + e.getMessage());
        }
    }
}