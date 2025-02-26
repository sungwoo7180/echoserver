package echoclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class EchoServer {
    public static void main(String[] args) {
        ServerSocket server = null;
        try {
            server = new ServerSocket(12346); // 1. socket() 2. bind() 3. listen()
            System.out.println("Server is running...");
            while (true) {
                Socket socket = server.accept(); // 4. accept()
                System.out.println("Client connected");
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
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (server != null && !server.isClosed()) {
                try {
                    server.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}