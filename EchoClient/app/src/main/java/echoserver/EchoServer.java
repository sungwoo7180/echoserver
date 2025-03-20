package echoserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class EchoServer {

    private static final int MAX_BYTES = 14; // 바이트 크기 제한

    public static void main(String[] args) {
        ServerSocket server = null;
        try {
            // 1. socket() 2. bind() 3. listen()
            server = new ServerSocket(12346);
            System.out.println("Server is running...");
            while (true) {
                // 4. accept() 는 blocking method 입니다.
                // 즉 클라이언트가 연결 요청을 보내기 전까지 여기서 무한 대기 상태.
                Socket socket = server.accept();

                System.out.println("Client connected: " + socket.getRemoteSocketAddress());

                /*
                // 클라이언트로 데이터를 읽어들이기 위한 BufferedReader 객체 생성 ( Buffer : 문자열 )
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                // 클라이언트로 데이터를 전송하기 위한 PrintWriter 객체 생성
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                String line;
                while ((line = reader.readLine()) != "quit") {
                    System.out.println("Received: " + line);
                    writer.println(line);
                }
                System.out.println("Client disconnected");
                socket.close();
                */

                handleClient(socket);
            }
        } catch (BindException e) {
            System.err.println("[ERROR] 포트 바인딩 실패: " + e.getMessage());
            System.err.println("포트가 이미 사용 중이거나 관리자 권한이 필요합니다.");
        } catch (SocketException e) {
            System.err.println("[ERROR] 소켓 오류: " + e.getMessage());
            System.err.println("FD 부족, 네트워크 오류, 버퍼 문제 가능성 있음.");
        } catch (IOException e) {
            System.err.println("[ERROR] 서버 소켓 생성 실패: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (server != null && !server.isClosed()) {
                try {
                    server.close();
                } catch (IOException e) {
                    System.err.println("[ERROR] 서버 소켓 닫기 실패: " + e.getMessage());
                }
            }
        }
    }

    private static void handleClient(Socket socket) {
        try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                String clientInfo = socket.getRemoteSocketAddress().toString();

                String validationError = validateMessage(line);
                if (validationError != null) {
                    writer.println("[ERROR] " + validationError);
                    System.out.println("[ERROR] 클라이언트(" + clientInfo + ")에서 " + validationError + " 전송: \"" + line + "\"");
                    continue;
                }

                // 정상 메시지라면 그대로 응답
                System.out.println("Received from " + clientInfo + ": " + line);
                writer.println(line);

                if ("quit".equalsIgnoreCase(line)) {
                    break;
                }
            }
            System.out.println("Client disconnected: " + socket.getRemoteSocketAddress());
        } catch (IOException e) {
            System.err.println("클라이언트 통신 중 오류 발생.");
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("클라이언트 소켓 닫기 실패.");
                e.printStackTrace();
            }
        }
    }

    // 메시지 검증 함수 (클라이언트와 동일한 로직)
    private static String validateMessage(String line) {
        if (isEmpty(line)) {
            return "메시지는 공백일 수 없습니다.";
        }
        if (isOverMaxBytes(line)) {
            return "메시지가 너무 깁니다. " + MAX_BYTES + "바이트 이하로 입력하세요.";
        }
        return null; // 유효한 메시지
    }

    // 공백 혹은 빈 문자열 방지
    private static boolean isEmpty(String line) {
        return line.trim().isEmpty();
    }

    // 메시지의 바이트 크기 검사
    private static boolean isOverMaxBytes(String line) {
        try {
            return line.getBytes("UTF-8").length > MAX_BYTES;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return true;
        }
    }

}