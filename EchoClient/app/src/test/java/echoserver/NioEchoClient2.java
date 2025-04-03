package echoserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Scanner;

public class NioEchoClient2 {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 12345;
    private static final int BUFFER_SIZE = 256;
    private static final int CHUNK_SIZE = 50; // 한 번에 전송할 최대 메시지 크기 (Mode 2)
    private static final int MAX_MESSAGE_SIZE = 200; // 허용되는 최대 메시지 길이 (Mode 1)

    private static final int MODE = 2; // 1 = 거절 모드, 2 = 쪼개기 모드

    public static void main(String[] args) {
        try (Selector selector = Selector.open();
             SocketChannel socketChannel = SocketChannel.open();
             Scanner scanner = new Scanner(System.in)) {

            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress(SERVER_ADDRESS, SERVER_PORT));
            socketChannel.register(selector, SelectionKey.OP_CONNECT);

            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            boolean waitingForResponse = false; // 서버 응답 대기 여부

            while (true) {
                selector.select();
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (key.isConnectable()) {
                        handleConnect(key);
                    } else if (key.isReadable()) {
                        waitingForResponse = handleRead(key, buffer);
                    }
                }

                if (!waitingForResponse) {
                    System.out.print("입력: ");
                    if (!scanner.hasNextLine()) continue;

                    String userInput = scanner.nextLine().trim();

                    if (userInput.isEmpty()) {
                        System.out.println("!!빈 메시지는 전송할 수 없습니다!");
                        waitingForResponse = false; // 오류 시 대기 상태 해제
                        continue;
                    }

                    // 메시지 길이 초과 처리 (Mode 1)
                    if (MODE == 1 && userInput.length() > MAX_MESSAGE_SIZE) {
                        System.out.println("!!메시지가 너무 큽니다! 최대 " + MAX_MESSAGE_SIZE + "자까지 가능합니다.");
                        waitingForResponse = false; // 오류 시 대기 상태 해제
                        continue;
                    }

                    waitingForResponse = true;

                    // Mode 에 맞게 메시지 전송
                    if (MODE == 1) {
                        handleWrite(socketChannel, buffer, userInput); // Mode 1: 거절 모드
                    } else if (MODE == 2) {
                        sendChunkedMessage(socketChannel, buffer, userInput); // Mode 2: 쪼개기 모드
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleConnect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        if (channel.finishConnect()) {
            System.out.println("서버 연결 완료");
            channel.register(key.selector(), SelectionKey.OP_READ);
        }
    }

    private static boolean handleRead(SelectionKey key, ByteBuffer buffer) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        buffer.clear(); // 버퍼 초기화

        int bytesRead = channel.read(buffer);
        if (bytesRead == -1) {
            channel.close();
            return false;
        }

        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String message = new String(bytes).trim();
        System.out.println("서버 응답: " + message);

        return false;
    }

    private static void handleWrite(SocketChannel channel, ByteBuffer buffer, String userInput) throws IOException {
        buffer.clear();
        buffer.put(userInput.getBytes());
        buffer.flip();

        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void sendChunkedMessage(SocketChannel channel, ByteBuffer buffer, String message) throws IOException {
        message += "|"; // 메시지 끝 표시
        int length = message.length();
        int start = 0;
        while (start < length) {
            int end = Math.min(start + CHUNK_SIZE, length);
            String chunk = message.substring(start, end);
            handleWrite(channel, buffer, chunk);
            System.out.println("**메시지 조각 전송: " + chunk);
            start = end;
        }
    }
}
