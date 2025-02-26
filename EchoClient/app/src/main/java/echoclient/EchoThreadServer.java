package echoclient;

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

        // [ 1. socket() : ServerSocket 생성: 12345 포트 바인딩 및 리스닝 ]
        try (ServerSocket server = new ServerSocket(12345)) {
            System.out.println("Server is running...");

            // 클라이언트 연결 무한 대기
            while (true) {
                // accept(): 클라이언트 연결 수락 (Blocking I/O)
                Socket socket = server.accept();
                System.out.println("Client connected from " + socket.getRemoteSocketAddress());

                // 4. 스레드 풀에 작업 제출 (각 클라이언트 별 독립 스레드)
                threadPool.submit(() -> handleClient(socket));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            threadPool.shutdown(); // 스레드 풀 종료
        }
    }

    private static void handleClient(Socket socket) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        String clientId = socket.getRemoteSocketAddress().toString();

        // 5. 자원 자동 해제를 위한 try-with-resources
        // finally 에서 명시적으로 닫던 방식을 개선 ( 컴파일러가 자동으로 최적화된 finally 블록 생성 )
        // writer.close() -> reader.close()-> socket.close() 순으로 자동 호출
        try (socket; // Java 9+: Socket을 자동으로 닫음
                // 6. 입력 스트림: 바이트 → 문자 → 버퍼링 (효율적 읽기)
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                // 7. 출력 스트림: autoFlush=true (println 시 즉시 전송)
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            String line;
            System.out.printf("[%s] %s Connected\n", sdf.format(new Date()), clientId);
            // 8. 클라이언트 데이터 읽기 (readLine은 개행 기준)
            while ((line = reader.readLine()) != null) { // 클라이언트 연결 종료 시 null 반환
                System.out.printf("[%s] From %s: %s\n", sdf.format(new Date()), clientId, line);

                // 9. "quit" 수신 시 연결 종료
                if ("quit".equalsIgnoreCase(line)) {
                    writer.println("Server closing connection");
                    break;
                }

                // 10. 에코 응답 전송
                writer.println("ECHO: " + line);
            }
            System.out.println("Client disconnected: " + socket.getRemoteSocketAddress());
        } catch (IOException e) {
            System.err.println("Client handling error: " + e.getMessage());
        }
    }
}