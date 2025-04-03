package echoserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Scanner;
import java.util.concurrent.*;

public class NIOMultiThreadEchoClient2 {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 12345;
    private static final ExecutorService executor = Executors.newCachedThreadPool(); // CachedThreadPool 사용

    public static void main(String[] args) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(SERVER_ADDRESS, SERVER_PORT));
            channel.configureBlocking(true);
            System.out.println("Connected to Echo server.");

            // 서버 응답 수신 스레드 시작
            executor.submit(new ReaderThread(channel));

            // 사용자 입력 처리
            try (Scanner scanner = new Scanner(System.in)) {
                while (true) {
                    System.out.print("> ");
                    String message = scanner.nextLine();

                    if ("quit".equalsIgnoreCase(message)) {
                        System.out.println("클라이언트를 종료합니다.");
                        break;
                    }

                    // 각 사용자 입력에 대해 새로운 WriterThread를 제출
                    executor.submit(new WriterThread(channel, message));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class ReaderThread implements Runnable {
        private final SocketChannel channel;

        public ReaderThread(SocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public void run() {
            ByteBuffer readBuffer = ByteBuffer.allocate(1024);
            try {
                while (true) {
                    readBuffer.clear();
                    int bytesRead = channel.read(readBuffer);
                    if (bytesRead == -1) {
                        System.out.println("Server closed connection.");
                        System.exit(0);
                    }
                    if (bytesRead > 0) {
                        readBuffer.flip();
                        byte[] data = new byte[bytesRead];
                        readBuffer.get(data);
                        String echo = new String(data);
                        System.out.println("서버로부터 Echo 수신: " + echo);
                    }
                }
            } catch (IOException e) {
                System.err.println("서버 응답 수신 중 예외 발생 - " + e.getMessage());
            }
        }
    }

    static class WriterThread implements Runnable {
        private final SocketChannel channel;
        private final String message;

        public WriterThread(SocketChannel channel, String message) {
            this.channel = channel;
            this.message = message;
        }

        @Override
        public void run() {
            try {
                ByteBuffer writeBuffer = ByteBuffer.wrap(message.getBytes());
                while (writeBuffer.hasRemaining()) {
                    channel.write(writeBuffer);
                }
                System.out.println("메시지 전송 완료: " + message);
            } catch (IOException e) {
                System.err.println("메시지 전송 중 예외 발생 - " + e.getMessage());
            }
        }
    }
}
