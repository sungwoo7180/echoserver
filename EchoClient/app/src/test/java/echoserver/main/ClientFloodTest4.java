package echoserver.main;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientFloodTest4 {
    private static final int CLIENTS = 100;
    private static final int MESSAGE_COUNT = 3;
    private static final int EXPECTED_TOTAL = CLIENTS * MESSAGE_COUNT * CLIENTS; // 3만

    public static void main(String[] args) throws InterruptedException {
        ExecutorService pool = Executors.newCachedThreadPool();
        CountDownLatch readyLatch = new CountDownLatch(CLIENTS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(CLIENTS);
        AtomicInteger totalReceived = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        long startUsedMemory = getUsedMemory();

        // 클라이언트 실행
        for (int i = 0; i < CLIENTS; i++) {
            int id = i;
            pool.submit(() -> runClient(id, readyLatch, startLatch, endLatch, totalReceived));
        }

        // 모든 클라이언트 연결 대기
        readyLatch.await();
        System.out.println("\uD83D\uDCA5 모든 클라이언트 연결 완료. 요이땅!");

        // 주기적인 실시간 모니터링 쓰레드
        Thread monitorThread = new Thread(() -> {
            try {
                while (true) {
                    long currentTime = System.currentTimeMillis();
                    long elapsed = currentTime - startTime;
                    long usedMemory = getUsedMemory() - startUsedMemory;

                    System.out.println("[모니터] 수신 메시지 수: " + totalReceived.get() +
                            " / 예상: " + EXPECTED_TOTAL +
                            " | 시간: " + elapsed + "ms" +
                            " | 메모리: " + (usedMemory / (1024 * 1024)) + "MB");
                    Thread.sleep(1000);
                }
            } catch (InterruptedException ignored) {
            }
        });
        monitorThread.setDaemon(true); // 데몬으로 설정해서 메인 종료되면 자동 종료
        monitorThread.start();

        // 요이땅!
        startLatch.countDown();

        // 모든 클라이언트 종료 대기
        endLatch.await();

        long endTime = System.currentTimeMillis();
        long endUsedMemory = getUsedMemory();

        System.out.println("\n모든 클라이언트 종료 완료 ✅");
        System.out.println("총 실행 시간: " + (endTime - startTime) + " ms");
        System.out.println("최종 수신 메시지 수: " + totalReceived.get() + " / 예상: " + EXPECTED_TOTAL);
        System.out.println("메모리 사용량: " + ((endUsedMemory - startUsedMemory) / (1024 * 1024)) + " MB");
    }

    private static void runClient(int id, CountDownLatch readyLatch, CountDownLatch startLatch,
                                  CountDownLatch endLatch, AtomicInteger totalReceived) {
        String nickname = "user" + id;

        try  {
            SocketChannel sc = SocketChannel.open(new InetSocketAddress("localhost", 12345));
            sc.configureBlocking(true);
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            // 닉네임 전송
            sc.write(ByteBuffer.wrap((nickname + "\n").getBytes()));
            readyLatch.countDown(); // 준비 완료
            startLatch.await();     // 시작 신호 대기

            // 메시지 전송
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String msg = "Hello " + i + " from " + nickname;
                sc.write(ByteBuffer.wrap((msg + "\n").getBytes()));
                Thread.sleep(10);
            }

            // 수신 (5초간)
            long endTime = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < endTime) {
                int read = sc.read(buffer);
                if (read > 0) {
                    buffer.flip();
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);
                    //System.out.println("[" + nickname + " 수신] " + new String(data).trim());
                    totalReceived.incrementAndGet();
                    buffer.clear();
                } else {
                    // Thread.sleep(10);
                }
            }

        } catch (IOException | InterruptedException e) {
            System.err.println("[ERROR] client " + nickname + ": " + e.getMessage());
        } finally {
            endLatch.countDown();
        }
    }

    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
