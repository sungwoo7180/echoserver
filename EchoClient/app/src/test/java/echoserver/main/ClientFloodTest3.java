package echoserver.main;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;

public class ClientFloodTest3 {
    private static final int CLIENTS = 100;
    private static final int MESSAGE_COUNT = 3;

    public static void main(String[] args) throws InterruptedException {
        ExecutorService pool = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(CLIENTS);

        long startTime = System.currentTimeMillis();
        long startUsedMemory = getUsedMemory();

        for (int i = 0; i < CLIENTS; i++) {
            int id = i;
            pool.submit(() -> runClient(id, latch));
        }

        latch.await();
        pool.shutdown();

        long endTime = System.currentTimeMillis();
        long endUsedMemory = getUsedMemory();

        System.out.println("\n모든 클라이언트 종료 완료");
        System.out.println("총 실행 시간: " + (endTime - startTime) + " ms");
        System.out.println("메모리 사용량: " + ((endUsedMemory - startUsedMemory) / (1024 * 1024)) + " MB");
    }

    private static void runClient(int id, CountDownLatch latch) {
        String nickname = "user" + id;

        try (SocketChannel sc = SocketChannel.open(new InetSocketAddress("localhost", 12345))) {
            sc.configureBlocking(true);
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            // 닉네임 전송
            sc.write(ByteBuffer.wrap((nickname + "\n").getBytes()));
            Thread.sleep(10);

            // 메시지 전송
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String msg = "Hello " + i + " from " + nickname;
                sc.write(ByteBuffer.wrap((msg + "\n").getBytes()));
                Thread.sleep(10);
            }

            // 수신 모니터 쓰레드
            ExecutorService readerPool = Executors.newSingleThreadExecutor();
            Future<?> future = readerPool.submit(() -> {
                int receivedCount = 0;
                long readUntil = System.currentTimeMillis() + 5000; // 5초 대기

                try {
                    while (System.currentTimeMillis() < readUntil) {
                        int read = sc.read(buffer);
                        if (read > 0) {
                            buffer.flip();
                            byte[] data = new byte[buffer.remaining()];
                            buffer.get(data);
                            System.out.println("[" + nickname + " 수신] " + new String(data).trim());
                            buffer.clear();
                            receivedCount++;
                        } else {
                            Thread.sleep(10);
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    System.err.println("[ERROR] client " + nickname + " 읽기 오류: " + e.getMessage());
                }
            });

            // 6초 내로 수신 쓰레드 종료 유도
            try {
                future.get(6, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                future.cancel(true);
            } finally {
                readerPool.shutdownNow();
                sc.close();
                latch.countDown();
            }

        } catch (IOException | InterruptedException | ExecutionException e) {
            System.err.println("[ERROR] client " + nickname + ": " + e.getMessage());
            latch.countDown();
        }
    }

    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
