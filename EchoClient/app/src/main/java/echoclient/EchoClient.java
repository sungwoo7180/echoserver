package echoclient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class EchoClient {
    public static void main(String[] args) {
        Socket socket = null;
        Scanner scanner = new Scanner(System.in);

        try {
            socket = new Socket("localhost", 12346); // 서버의 IP:localhost 포트:12345로 연결
            System.out.println("Connected to server");
            // 서버로 데이터를 읽어들이기 위한 BR 객체 생성
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            // 서버로 데이터를 전송하기 위한 PW 객체 생성
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

            String line;
            while (true) {
                System.out.print("Enter text: ");
                line = scanner.nextLine();
                writer.println(line);
                if ("quit".equalsIgnoreCase(line)) {
                    break;
                }
                System.out.println("Received: " + reader.readLine());
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        } finally {
            if (socket != null && !socket.isClosed()) {
                try {
                    socket.close();
                    scanner.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
