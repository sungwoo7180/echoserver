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
            server = new ServerSocket(12346); // 1 socket() 2 bind() 3 listen()
            System.out.println("Server is running...");
            while (true) {
                Socket socket = server.accept(); // 4 accept()
                System.out.println("Client connected");
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                String line;
                while ((line = reader.readLine()) != null) {
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