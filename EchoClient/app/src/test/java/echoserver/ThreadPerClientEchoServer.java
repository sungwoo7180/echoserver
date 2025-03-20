package echoserver;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ThreadPerClientEchoServer {
    public static void main(String[] args) {
        try ( ServerSocket server = new ServerSocket(12345)) {
            System.out.println("서버 실행중");
            while (true) {
                Socket socket = server.accept();

            }
        } catch (IOException e) {

        }
    }
}

