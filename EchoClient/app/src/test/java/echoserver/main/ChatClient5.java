package echoserver.main;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

public class ChatClient5 {
    public static void main(String[] args) {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress("localhost", 12345));
            channel.configureBlocking(true);

            Scanner scanner = new Scanner(System.in);

            while (true) {
                System.out.print("닉네임을 입력하세요: ");
                String nickname = scanner.nextLine().trim();
                send(channel, nickname + "\n");

                String response = receive(channel);
                System.out.println("[서버 응답] " + response);
                if (!response.contains("중복")) break;
            }

            Thread reader = new Thread(() -> {
                try {
                    while (true) {
                        String msg = receive(channel);
                        if (msg == null) break;
                        System.out.println("[수신] " + msg);
                        System.out.print("> ");
                    }
                } catch (IOException e) {
                    System.err.println("서버 응답 오류: " + e.getMessage());
                }
            });
            reader.setDaemon(true);
            reader.start();

            while (true) {
                System.out.print("> ");
                String input = scanner.nextLine();
                if ("quit".equalsIgnoreCase(input)) break;
                send(channel, input + "\n");
            }

        } catch (IOException e) {
            System.err.println("클라이언트 오류: " + e.getMessage());
        }
    }

    private static void send(SocketChannel channel, String msg) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(msg.getBytes());
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static String receive(SocketChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int read = channel.read(buffer);
        if (read == -1) return null;
        buffer.flip();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        return new String(data).trim();
    }
}
