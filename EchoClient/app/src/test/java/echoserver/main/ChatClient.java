package echoserver.main;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChatClient {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 12345;
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(SERVER_ADDRESS, SERVER_PORT));
            channel.configureBlocking(true);
            System.out.println("서버에 연결되었습니다.");

            Scanner scanner = new Scanner(System.in);

            // 닉네임 입력 및 전송
            while (true) {
                System.out.print("닉네임을 입력하세요: ");
                String nickname = scanner.nextLine().trim();
                sendMessage(channel, nickname + "\n"); // \n 추가

                String response = receiveMessage(channel);
                if (response.contains("중복된 닉네임")) {
                    System.out.println("[서버 응답] " + response);
                } else {
                    System.out.println("[서버 응답] " + response);
                    break; // 성공적으로 등록됨
                }
            }

            // 서버 응답 수신 스레드 시작
            executor.submit(new ReaderThread(channel));

            // 사용자 입력 → 메시지 전송
            while (true) {
                System.out.print("> ");
                String message = scanner.nextLine();
                if ("quit".equalsIgnoreCase(message)) {
                    System.out.println("클라이언트를 종료합니다.");
                    break;
                }
                executor.submit(new WriterThread(channel, message));
            }

        } catch (IOException e) {
            System.err.println("서버와 연결 실패: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    private static void sendMessage(SocketChannel channel, String message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes());
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static String receiveMessage(SocketChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int bytesRead = channel.read(buffer);
        if (bytesRead == -1) {
            throw new IOException("서버에서 연결 종료");
        }
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes).trim();
    }

    static class ReaderThread implements Runnable {
        private final SocketChannel channel;

        public ReaderThread(SocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public void run() {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            try {
                while (true) {
                    buffer.clear();
                    int bytesRead = channel.read(buffer);
                    if (bytesRead == -1) {
                        System.out.println("[서버 연결 종료]");
                        System.exit(0);
                    }
                    buffer.flip();
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);
                    System.out.println("\n[수신] " + new String(data).trim());
                    System.out.print("> ");
                }
            } catch (IOException e) {
                System.err.println("서버 응답 수신 중 오류: " + e.getMessage());
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
                ByteBuffer buffer = ByteBuffer.wrap((message + "\n").getBytes()); // \n 추가
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
            } catch (IOException e) {
                System.err.println("메시지 전송 중 오류: " + e.getMessage());
            }
        }
    }
}
