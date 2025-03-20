package echoserver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class EchoThreadClient {
    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        // 자원 자동 해제를 위한 try-with-resources
        // try (Socket socket = new Socket("192.168.30.", 12345)) { // 서버 포트 12345로 수정
        try (Socket socket = new Socket("localhost", 12345)) { // 서버 포트 12345로 수정
            System.out.println("Connected to server: " + socket.getRemoteSocketAddress());

            // 3. 입력 스트림: 바이트 → 문자 → 버퍼링(문자열)
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));
            // 4. 출력 스트림: autoFlush=true 설정
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

            String userInput;
            while (true) {
                System.out.print("Enter text: ");
                userInput = scanner.nextLine();

                // 5. 사용자 입력 전송
                writer.println(userInput);

                // 6. "quit" 입력 시 종료
                if ("quit".equalsIgnoreCase(userInput)) {
                    System.out.println("Closing connection");
                    break;
                }

                // 7. 서버 응답 읽기 (Blocking I/O)
                String response = reader.readLine();
                if (response == null) { // 서버 연결 종료 감지
                    System.out.println("Server closed connection");
                    break;
                }
                System.out.println("Server: " + response);
            }
        } catch (Exception e) {
            System.err.println("Client error: " + e.getMessage());
        } finally {
            scanner.close();
        }
    }
}