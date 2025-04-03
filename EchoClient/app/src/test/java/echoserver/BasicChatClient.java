package echoserver;

import java.io.*;
import java.lang.Thread;
import java.net.*;

public class BasicChatClient {
    private static final String SERVER_IP = "127.0.0.1";
    private static final int SERVER_PORT = 12346;

    public static void main(String[] args) {
        System.out.println("!!!! 채팅 클라이언트를 시작합니다. !!!!");

        try (Socket socket = new Socket(SERVER_IP, SERVER_PORT);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);  
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in))) {  

            // 이름 입력
            System.out.print("**** 이름을 입력하세요: ");
            String name = userInput.readLine();
            out.println(name);

            // 서버로부터 방 목록 출력
            String roomList;
            while (!(roomList = in.readLine()).contains("입장할 방 이름을 입력하세요:")) {
                System.out.println(roomList);
            }

            // 입장할 방 입력
            System.out.print("**** 입장할 방 이름을 입력하세요: ");
            String roomName = userInput.readLine();
            out.println(roomName);
            System.out.println("!!!! '" + roomName + "' 방에 입장하였습니다. !!!!");

            // 서버에서 방 입장 메시지 수신
            String enterMessage;
            while (!(enterMessage = in.readLine()).contains("메시지를 입력하세요")) {
                System.out.println(enterMessage);
            }

            // 메시지 입력 가능 안내
            System.out.println("&&&& 이제 메시지를 입력할 수 있습니다. (종료하려면 /exit 입력) &&&&");

            // 서버의 메시지를 수신하는 별도 스레드 실행 (다른 사용자 메시지 수신)
            java.lang.Thread receiveThread = new Thread(() -> {
                try {
                    String serverMessage;
                    while ((serverMessage = in.readLine()) != null) {
                        // 본인이 입력 중일 때 깔끔하게 출력되도록 조정
                        System.out.print("\r" + " ".repeat(50) + "\r"); // 기존 입력 라인 삭제
                        System.out.println(serverMessage);
                        System.out.print("[" + name + "] : "); // 본인의 입력 UI 유지
                        System.out.flush();
                    }
                } catch (IOException e) {
                    System.out.println("!!!! 서버와의 연결이 끊어졌습니다. !!!!");
                }
            });
            receiveThread.start();

            // 메시지 입력 루프
            while (true) {
                System.out.print("[" + name + "] : "); // 본인이 입력할 때 UI 유지
                System.out.flush();
                String userMessage = userInput.readLine();

                if (userMessage.equalsIgnoreCase("/exit")) {
                    System.out.println("!!!! 채팅을 종료합니다. !!!!");
                    break;
                }

                out.println(userMessage);
                out.flush();
            }

        } catch (IOException e) {
            System.out.println("!!!! 서버 연결 오류: " + e.getMessage() + " !!!!");
        }
    }
}
