package echoserver;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ThreadPerClientEchoServer {
    public static void main(String[] args) {
        try (ServerSocket server = new ServerSocket(12345)) {
            System.out.println("Server is running...");

            while (true) {
                // .accept() 할때마다 새로운 스레드 생성
                Socket socket = server.accept();
                System.out.println("Client connected from " + socket.getRemoteSocketAddress());

                // 1. Thread 생성
                new ClientHandler1(socket).start();

                // 2. Runnable

                // 3. Ramda 를 이용
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

// 각 클라이언트를 처리할 전담 스레드 클래스
class ClientHandler1 extends Thread {
    private Socket socket;
    private SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");

    public ClientHandler1(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        String clientId = socket.getRemoteSocketAddress().toString();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

            System.out.printf("[%s] %s Connected\n", sdf.format(new Date()), clientId);

            String line;
            while ((line = reader.readLine()) != null) {
                System.out.printf("[%s] From %s: %s\n", sdf.format(new Date()), clientId, line);

                if ("quit".equalsIgnoreCase(line)) {
                    writer.println("Server closing connection");
                    break;
                }

                writer.println("ECHO: " + line);
            }

            System.out.println("Client disconnected: " + clientId);
        } catch (IOException e) {
            System.err.println("Client handling error: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

class ClientHandler2 implements Runnable {

    Socket socket;

    public ClientHandler2(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {

     try ( BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
           PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()))
     ) {
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println("Client: " + line);
            if ("quit".equalsIgnoreCase(line)) {
                writer.println(line);
            }
        }
     } catch( Exception e ){
        e.printStackTrace();
     }

    }
}
