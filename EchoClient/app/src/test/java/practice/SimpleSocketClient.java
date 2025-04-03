package practice;

import java.io.*;
import java.net.Socket;

public class SimpleSocketClient {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;

        try (Socket socket = new Socket(host, port);
             BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in));
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {

            System.out.println("Connected to server. Type messages:");

            String message;
            while ((message = userInput.readLine()) != null) {
                out.write(message);
                out.newLine(); // 줄바꿈은 서버 쪽에서 메시지 경계 판단용
                out.flush();

                String response = in.readLine();
                System.out.println("Echoed from server: " + response);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
