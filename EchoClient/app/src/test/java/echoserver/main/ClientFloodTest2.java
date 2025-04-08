package echoserver.main;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;

public class ClientFloodTest2 {
    private static final int CLIENTS = 100;

    public static void main(String[] args) throws InterruptedException {
        ExecutorService pool = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(CLIENTS);

        long startTime = System.currentTimeMillis();
        long startUsedMemory = getUsedMemory();

        for (int i = 0; i < CLIENTS; i++) {
            int id = i;
            pool.submit(() -> {
                runClient(id);
                latch.countDown();
            });
        }

        latch.await();
        pool.shutdown();

        long endTime = System.currentTimeMillis();
        long endUsedMemory = getUsedMemory();

        System.out.println("\n모든 클라이언트 종료 완료");
        System.out.println("총 실행 시간: " + (endTime - startTime) + " ms");
        System.out.println("메모리 사용량: " + ((endUsedMemory - startUsedMemory) / (1024 * 1024)) + " MB");
    }

    private static void runClient(int id) {
        String nickname = "user" + id;

        try (SocketChannel sc = SocketChannel.open(new InetSocketAddress("localhost", 12345))) {
            sc.configureBlocking(false); // non-blocking 모드
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            // 닉네임 전송
            sc.write(ByteBuffer.wrap((nickname + "\n").getBytes()));
            Thread.sleep(10);

            // 메시지 전송
            for (int i = 0; i < 3; i++) {
                String msg = "Hello " + i + " from " + nickname;
                sc.write(ByteBuffer.wrap((msg + "\n").getBytes()));
                Thread.sleep(10);
            }

            // 일정 시간 동안 읽기 시도 (비동기적으로)
            long readUntil = System.currentTimeMillis() + 2000; // 2초 동안 읽기 시도
            while (System.currentTimeMillis() < readUntil) {
                int read = sc.read(buffer);
                if (read > 0) {
                    buffer.flip();
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);
                    System.out.println("[" + nickname + " 수신] " + new String(data));
                    buffer.clear();
                } else {
                    // 데이터를 못 읽었으면 잠시 대기
                    Thread.sleep(10);
                }
            }

        } catch (IOException | InterruptedException e) {
            System.err.println("[ERROR] client " + nickname + ": " + e.getMessage());
        }
    }


    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}