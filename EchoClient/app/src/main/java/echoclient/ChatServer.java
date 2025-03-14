package echoserver;

import java.io.*;
import java.net.*;
import java.util.*;

public class ChatServer {
    private static Map<String, Set<ClientHandler>> rooms = Collections.synchronizedMap(new HashMap<>());
    private static Map<String, List<String>> chatHistory = Collections.synchronizedMap(new HashMap<>());

    public static void main(String[] args) throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(12346)) {
            System.out.println("--Chat server started on port 12346...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                ClientHandler handler = new ClientHandler(clientSocket);
                handler.start();
            }
        }
    }

    static class ClientHandler extends Thread {
        private Socket socket;
        private PrintWriter out;
        private BufferedReader in;
        private String userName;
        private String roomName;

        public ClientHandler(Socket socket) throws IOException {
            this.socket = socket;
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }

        public void run() {
            try {
                // 클라이언트에서 받은 이름 사용
                userName = in.readLine();
                System.out.println("사용자 연결됨: " + userName);

                // 현재 존재하는 방 목록 전송
                sendRoomList();

                // 클라이언트가 방 선택
                roomName = in.readLine();
                rooms.computeIfAbsent(roomName, r -> new HashSet<>()).add(this);
                chatHistory.computeIfAbsent(roomName, r -> new ArrayList<>());

                System.out.println("'" + userName + "'님이 방 '" + roomName + "'에 입장하였습니다.");

                // 모든 클라이언트에게 입장 메시지 전송
                broadcastToRoom("[" + userName + "] 님이 입장하셨습니다.");

                // 채팅 기록 역순 전송
                sendChatHistory();

                // 메시지 수신 후 브로드캐스트
                String message;
                while ((message = in.readLine()) != null) {
                    String formattedMessage = "[" + userName + "] " + message;
                    chatHistory.get(roomName).add(formattedMessage);
                    broadcastToRoom(formattedMessage);
                }

            } catch (IOException e) {
                System.out.println("사용자 연결 종료: " + userName);
            } finally {
                leaveRoom();
                try {
                    socket.close();
                } catch (IOException ex) {
                    // 무시
                }
            }
        }

        // 서버의 모든 방과 인원수 전송
        private void sendRoomList() {
            out.println("현재 활성화된 방 목록:");
            if (rooms.isEmpty()) {
                out.println("현재 생성된 방이 없습니다.");
            } else {
                for (Map.Entry<String, Set<ClientHandler>> entry : rooms.entrySet()) {
                    out.println("방 이름: " + entry.getKey() + " | 참가자: " + entry.getValue().size() + "명");
                }
            }
            out.println("입장할 방 이름을 입력하세요:");
        }

        // 특정 방의 메시지 기록 역순 전송
        private void sendChatHistory() {
            List<String> history = chatHistory.get(roomName);
            if (history != null && !history.isEmpty()) {
                out.println("이전 채팅 기록 (최신순):");
                for (int i = history.size() - 1; i >= 0; i--) {
                    out.println(history.get(i));
                }
            } else {
                out.println("채팅 기록 없음.");
            }

            // 클라이언트가 입력을 시작할 수 있도록 신호 전송
            out.println("메시지를 입력하세요.");
            out.flush();
        }

        // 방에 있는 모든 사용자에게 메시지 전송 (완전 수정)
        private void broadcastToRoom(String message) {
            Set<ClientHandler> clients = rooms.get(roomName);
            if (clients != null) {
                for (ClientHandler ch : clients) {
                    ch.out.println(message); // 모든 클라이언트에게 메시지 전송
                }
            }
            System.out.println(message);  // 서버에서 로그 1번만 출력
        }

        // 클라이언트가 나갈 때 처리
        private void leaveRoom() {
            if (roomName != null && rooms.containsKey(roomName)) {
                rooms.get(roomName).remove(this);
                broadcastToRoom("[" + userName + "] 님이 퇴장하셨습니다.");
                if (rooms.get(roomName).isEmpty()) {
                    rooms.remove(roomName);
                    chatHistory.remove(roomName);
                    System.out.println("'" + roomName + "' 방 삭제됨 (모든 사용자 퇴장)");
                }
            }
        }
    }
}
