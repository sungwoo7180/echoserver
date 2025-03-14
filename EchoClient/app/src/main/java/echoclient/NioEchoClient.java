package echoserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Scanner;
// import java.util.Set;

public class NioEchoClient {
    private static final String SERVER_ADDRESS = "localhost";
    private static final int SERVER_PORT = 12345;
    private static final int BUFFER_SIZE = 1024;

    public static void main(String[] args) {
        try (Selector selector = Selector.open();
             SocketChannel socketChannel = SocketChannel.open();
             Scanner scanner = new Scanner(System.in); ) {

            socketChannel.configureBlocking(false);
            socketChannel.connect(new InetSocketAddress(SERVER_ADDRESS, SERVER_PORT));
            socketChannel.register(selector, SelectionKey.OP_CONNECT);

            
            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

            while (true) {
                selector.select();
                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();

                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();
                    keyIterator.remove();

                    if (key.isConnectable()) {
                        handleConnect(key);
                    } else if (key.isReadable()) {
                        handleRead(key, buffer);
                    } else if (key.isWritable()) {
                        System.out.print("입력: ");
                        String userInput = scanner.nextLine();
                        handleWrite(key, buffer, userInput);
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
            channel.register(key.selector(), SelectionKey.OP_WRITE);
        }
    }

    private static void handleRead(SelectionKey key, ByteBuffer buffer) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        buffer.clear();
        int bytesRead = channel.read(buffer);
        if (bytesRead == -1) {
            channel.close();
            System.out.println("서버 연결 종료");
        } else {
            buffer.flip();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            System.out.println("서버 응답: " + new String(bytes));
            channel.register(key.selector(), SelectionKey.OP_WRITE);
        }
    }

    private static void handleWrite(SelectionKey key, ByteBuffer buffer, String userInput) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        buffer.clear();
        buffer.put(userInput.getBytes());
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        if ("quit".equalsIgnoreCase(userInput)) {
            channel.close();
            System.out.println("Closing connection");
        } else {
            channel.register(key.selector(), SelectionKey.OP_READ);
        }
    }
}

