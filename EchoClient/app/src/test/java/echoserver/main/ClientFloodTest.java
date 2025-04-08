package echoserver.main;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ClientFloodTest {
    private static final int CLIENTS = 100;
    private static final int THREAD_POOL_SIZE = 20;

    public static void main(String[] args) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<Future<Boolean>> futures = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        long startUsedMemory = getUsedMemory();

        for (int i = 0; i < CLIENTS; i++) {
            int id = i;
            futures.add(pool.submit(() -> runClient(id)));
        }

        pool.shutdown();
        pool.awaitTermination(15, TimeUnit.SECONDS);

        // 완료 여부 확인
        long successCount = futures.stream().filter(f -> {
            try {
                return f.get(); // true/false 반환 확인
            } catch (Exception e) {
                return false;
            }
        }).count();

        long endTime = System.currentTimeMillis();
        long endUsedMemory = getUsedMemory();

        System.out.println("\n정상 종료된 클라이언트 수: " + successCount + "/" + CLIENTS);
        System.out.println("총 실행 시간: " + (endTime - startTime) + " ms");
        System.out.println("메모리 사용량: " + ((endUsedMemory - startUsedMemory) / (1024 * 1024)) + " MB");
    }

    private static boolean runClient(int id) {
        String nickname = "user" + id;

        try (SocketChannel sc = SocketChannel.open(new InetSocketAddress("localhost", 12345))) {
            sc.configureBlocking(true);
            ByteBuffer buffer = ByteBuffer.allocate(1024);

            // 닉네임 전송
            sc.write(ByteBuffer.wrap((nickname + "\n").getBytes()));
            Thread.sleep(1);

            // 간단한 메시지 전송
            for (int i = 0; i < 3; i++) {
                String msg = "Hello " + i + " from " + nickname;
                sc.write(ByteBuffer.wrap((msg + "\n").getBytes()));
                Thread.sleep(1);
            }

            // 서버 응답 수신 (Optional)
            while (sc.read(buffer) > 0) {
                buffer.clear();
            }

            return true;
        } catch (IOException | InterruptedException e) {
            System.err.println("[ERROR] client " + nickname + ": " + e.getMessage());
            return false;
        }
    }

    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
