package echoserver.main;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientFloodTest5 {
    private static final int CLIENTS = 100;
    private static final int MESSAGE_COUNT = 3;
    private static final int EXPECTED_TOTAL = CLIENTS * MESSAGE_COUNT * CLIENTS;

    private static final AtomicInteger totalReceived = new AtomicInteger();
    private static final CountDownLatch readyLatch = new CountDownLatch(CLIENTS);
    private static final CountDownLatch startLatch = new CountDownLatch(1);
    private static final CountDownLatch endLatch = new CountDownLatch(CLIENTS);

    public static void main(String[] args) throws InterruptedException {
        ExecutorService pool = Executors.newCachedThreadPool();

        long startUsedMemory = getUsedMemory();
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < CLIENTS; i++) {
            int id = i;
            pool.submit(() -> runClient(id));
        }

        // 모든 클라이언트가 연결될 때까지 대기
        readyLatch.await();
        System.out.println("\n[INFO] 모든 클라이언트 연결 완료. 메시지 전송 시작");
        startLatch.countDown();

        // 모니터 쓰레드: 수신 상태 주기적으로 출력
        Thread monitor = new Thread(() -> {
            try {
                long baseTime = System.currentTimeMillis();
                while (!Thread.currentThread().isInterrupted()) {
                    long usedMemory = (getUsedMemory() - startUsedMemory) / (1024 * 1024);
                    long elapsed = System.currentTimeMillis() - baseTime;
                    System.out.println("[모니터] 메시지 총합수: " + totalReceived.get()
                            + " / 예상: " + EXPECTED_TOTAL
                            + " | 시간: " + elapsed + "ms"
                            + " | 메모리: " + usedMemory + "MB");
                    Thread.sleep(1000);
                }
            } catch (InterruptedException ignored) {
            }
        });
        monitor.start();

        // 클라이언트 종료 대기
        endLatch.await();
        monitor.interrupt();

        long endTime = System.currentTimeMillis();
        long endUsedMemory = getUsedMemory();

        System.out.println("\n모든 클라이언트 종료 완료 ✅");
        System.out.println("총 실행 시간: " + (endTime - startTime) + " ms");
        System.out.println("최종 수신 메시지 수: " + totalReceived.get() + " / 예상: " + EXPECTED_TOTAL);
        System.out.println("메모리 사용량: " + ((endUsedMemory - startUsedMemory) / (1024 * 1024)) + " MB");
    }

    private static void runClient(int id) {
        String nickname = "user" + id;

        try (SocketChannel sc = SocketChannel.open(new InetSocketAddress("localhost", 12345))) {
            sc.configureBlocking(true);
            readyLatch.countDown();
            startLatch.await();

            // 닉네임 전송
            sc.write(ByteBuffer.wrap((nickname + "\n").getBytes()));

            // 메시지 전송
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String msg = "Hello " + i + " from " + nickname;
                sc.write(ByteBuffer.wrap((msg + "\n").getBytes()));
            }

            // 수신 쓰레드
            Thread reader = new Thread(() -> {
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                try {
                    while (true) {
                        int read = sc.read(buffer);
                        if (read > 0) {
                            buffer.flip();
                            byte[] data = new byte[buffer.remaining()];
                            buffer.get(data);
                            String msg = new String(data).trim();
                            System.out.println("[" + nickname + " 수신] " + msg);
                            totalReceived.incrementAndGet();
                            buffer.clear();
                        }
                    }
                } catch (IOException ignored) {
                }
            });
            reader.start();

            Thread.sleep(5000); // 5초 동안 수신 대기

            reader.interrupt();
            endLatch.countDown();
        } catch (IOException | InterruptedException e) {
            System.err.println("[ERROR] client " + nickname + ": " + e.getMessage());
            endLatch.countDown();
        }
    }

    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
