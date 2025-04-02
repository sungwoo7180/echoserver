package echoserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class NIOMultiThreadEchoClient {
    private static volatile boolean waitingForResponse = false;
    private static final Object lock = new Object();

    public static void main(String[] args) throws IOException {
        // 서버 연결 설정
        SocketChannel channel = SocketChannel.open();
        channel.connect(new InetSocketAddress("localhost", 12345));
        channel.configureBlocking(true);
        System.out.println("Connected to Echo server.");

        // 서버 응답 수신 스레드 시작 (올바른 Thread 사용)
        Thread readerThread = new Thread(new ReaderThread(channel));
        readerThread.setDaemon(true);
        readerThread.start();

        // 사용자 입력 처리
        try (java.util.Scanner scanner = new java.util.Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                String message = scanner.nextLine();

                if ("quit".equalsIgnoreCase(message)) {
                    System.out.println("클라이언트를 종료합니다.");
                    break;
                }

                synchronized (lock) {
                    while (waitingForResponse) {
                        try {
                            lock.wait();
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    waitingForResponse = true;
                }

                // 메시지 전송
                ByteBuffer writeBuffer = ByteBuffer.wrap(message.getBytes());
                while (writeBuffer.hasRemaining()) {
                    channel.write(writeBuffer);
                }
                System.out.println("메시지 전송 완료: " + message);
            }
        }

        channel.close();
    }

    // ReaderThread 클래스 수정
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

                        synchronized (lock) {
                            waitingForResponse = false;
                            lock.notify();
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("오류: 서버 응답 수신 중 예외 발생 - " + e.getMessage());
            }
        }
    }
}