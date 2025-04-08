package echoserver.main;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;

class ChatServerIntegrationTest {

    private static final int TEST_PORT = 33333;
    private static final String TEST_CHAT_HISTORY = "test_chat_history.txt";
    private static final String TEST_NICKNAMES = "test_nicknames.txt";
    private static Thread serverThread;

    @BeforeAll
    static void startServer() {
        ChatServer3 server = new ChatServer3(TEST_PORT, 5, 1024, TEST_CHAT_HISTORY, TEST_NICKNAMES);
        serverThread = new Thread(server::run, "ChatServer-Thread");
        serverThread.start();
    }

    @AfterAll
    static void stopServer() throws InterruptedException {
        serverThread.interrupt();
        serverThread.join();
    }

    @Test
    void testNicknameRegistrationAndChat() throws IOException {
        SocketChannel client = SocketChannel.open();
        client.connect(new InetSocketAddress("localhost", TEST_PORT));
        client.configureBlocking(true);

        String nickname = "tester_" + UUID.randomUUID();
        writeLine(client, nickname);
        String response = readUntilContains(client, "등록 완료", 20);
        Assertions.assertTrue(response.contains("등록 완료"));

        writeLine(client, "Hello World!");
        String chatBroadcast = readUntilContains(client, nickname + ": Hello World", 20);
        Assertions.assertTrue(chatBroadcast.contains(nickname + ": Hello World"));

        client.close();
    }

    private void writeLine(SocketChannel channel, String line) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap((line + "\n").getBytes());
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private String readUntilContains(SocketChannel channel, String expected, int maxAttempts) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        StringBuilder builder = new StringBuilder();

        int attempts = 0;
        while (attempts++ < maxAttempts) {
            buffer.clear();
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) break;

            buffer.flip();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            builder.append(new String(bytes).trim());

            if (builder.toString().contains(expected)) {
                break;
            }

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return builder.toString();
    }
}
