package echoserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NIOMultiThreadEchoServer {
    private static final int PORT = 12345;
    // 쓰기 대기 키 모음 (중복 방지 위해 Set 사용)
    private static final Set<SelectionKey> pendingWriteKeys = Collections.synchronizedSet(new HashSet<>());
    // 각 SelectionKey 별로 보낼 데이터를 보관하는 맵
    private static final Map<SelectionKey, Queue<ByteBuffer>> pendingData = new HashMap<>();
    // Selector 는 단일 스레드에서 관리 (동시 접근 방지)
    private static Selector selector;

    // TODO 1 : Concurrent.lock 을 이용해서 고급 동기화를 적용해보자!
    //
    public static void main(String[] args) throws IOException {

        try(Selector selector = Selector.open(); // 서버 채널과 셀렉터 초기화
            ServerSocketChannel serverChannel = ServerSocketChannel.open();) { // 1. socket() : 서버 소켓 생성

            serverChannel.bind(new InetSocketAddress(PORT));    // 2. bind() : 포트 바인딩
            serverChannel.configureBlocking(false);             // 논블로킹 모드 설정

            // 서버 소켓 채널은 OP_ACCEPT 만 관심 등록
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("Echo server started on port " + PORT);

            // Selector 루프 (단일 스레드에서 처리)
            while (true) {

                selector.select(); // I/O 이벤트 대기
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove(); // 현재 키 처리 예정이므로 목록에서 제거

                    try {
                        if (key.isAcceptable()) {
                            // 새 클라이언트 연결 수락
                            ServerSocketChannel server = (ServerSocketChannel) key.channel();
                            SocketChannel clientChannel = server.accept();
                            if (clientChannel != null) {
                                clientChannel.configureBlocking(false);
                                // 클라이언트 채널을 OP_READ 관심으로 등록
                                SelectionKey clientKey = clientChannel.register(selector, SelectionKey.OP_READ);
                                System.out.println("Client connected: " + clientChannel.getRemoteAddress());
                            }
                        }
                        else if (key.isReadable()) {
                            // 클라이언트로부터 데이터 수신 가능
                            SocketChannel clientChannel = (SocketChannel) key.channel();
                            ByteBuffer buffer = ByteBuffer.allocate(1024);
                            int bytesRead = clientChannel.read(buffer);
                            if (bytesRead == -1) {
                                // 클라이언트 연결 종료 처리
                                System.out.println("Client disconnected: " + clientChannel.getRemoteAddress());
                                cleanupKey(key);
                                continue;
                            }
                            if (bytesRead == 0) {
                                // 읽을 데이터 없음 (논블로킹모드에서 이 경우 발생 가능)
                                continue;
                            }
                            // 읽은 데이터를 준비하여 에코 전송
                            buffer.flip();
                            // [문제] 한번에 큰 메시지를 보낸 경우 채널의 send buffer 가 꽉 차면 한 번의 write 로 모두 전송되지 않을 수 있음
                            // [해결] write를 호출하여 최대한 전송하고, 다 보내지 못한 경우 나머지를 write 대기 목록에 저장
                            SocketChannel channel = (SocketChannel) key.channel();
                            ByteBuffer echoBuffer = buffer.duplicate(); // 에코 보낼 데이터
                            int bytesWritten = channel.write(echoBuffer);
                            if (echoBuffer.hasRemaining()) {
                                // 아직 전송되지 않은 데이터가 있음 -> 나머지 데이터를 write 대기열에 추가
                                ByteBuffer remainingData = ByteBuffer.allocate(echoBuffer.remaining());
                                remainingData.put(echoBuffer);
                                remainingData.flip();
                                // pendingData 맵에 저장 (키별로 큐 관리)
                                pendingData.computeIfAbsent(key, k -> new LinkedList<>()).add(remainingData);
                                // [문제] 기존 코드에서 writeQueue에 동일 키가 중복 추가되어 중복 Echo 발생
                                // [해결] 키별 Set(pendingWriteKeys)로 이미 등록된 키는 추가하지 않도록 관리
                                if (!pendingWriteKeys.contains(key)) {
                                    pendingWriteKeys.add(key);
                                    // OP_WRITE 관심등록 추가 (중복 추가 방지)
                                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                                }
                            } else {
                                // 모두 전송 완료. OP_WRITE 관심 필요 없음, OP_READ만 유지
                                key.interestOps(SelectionKey.OP_READ);
                            }
                        }
                        else if (key.isWritable()) {
                            // OP_WRITE 이벤트: 채널이 쓸 준비가 되었음
                            SocketChannel clientChannel = (SocketChannel) key.channel();
                            Queue<ByteBuffer> queue = pendingData.get(key);
                            // [문제] 여러 스레드에서 OP_WRITE 처리를 경쟁적으로 수행하여 상태 꼬임 발생
                            // [해결] Selector 스레드에서만 OP_WRITE 처리를 수행하고, pendingData에 대기 중인 데이터가 있을 때만 write 수행
                            if (queue != null && !queue.isEmpty()) {
                                // 대기 중인 모든 데이터를 전송 시도
                                ByteBuffer buf = queue.peek();
                                clientChannel.write(buf);
                                if (buf.hasRemaining()) {
                                    // 아직 다 못 보냈으면 다음에 이어서 전송 (OP_WRITE 유지)
                                    // 남은 데이터가 있어도 이미 interestOps에 OP_WRITE 등록되어 있음
                                } else {
                                    // 현재 버퍼 전송 완료 -> 큐에서 제거
                                    queue.poll();
                                }
                            }
                            // 남은 대기 데이터가 없다면 OP_WRITE 관심 제거
                            if (queue == null || queue.isEmpty()) {
                                // 더 이상 보낼 데이터가 없으므로 OP_WRITE 비활성화 (OP_READ만 활성)
                                key.interestOps(SelectionKey.OP_READ);
                                pendingWriteKeys.remove(key);
                            } else {
                                // 큐에 아직 데이터가 남아 있으면 OP_WRITE 유지 (다음 루프에서 계속 처리)
                                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                            }
                        }
                    } catch (IOException ex) {
                        // 예외 발생 시 해당 키 정리 (예: 클라이언트 연결 강제 종료 등)
                        cleanupKey(key);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static void cleanupKey(SelectionKey key) {
        try {
            // 채널 닫고 키 취소
            key.cancel();
            key.channel().close();
        } catch (IOException e) {
            System.err.println("Channel close error: " + e.getMessage());
        }
        // 관련된 pending 데이터 정리
        pendingWriteKeys.remove(key);
        pendingData.remove(key);
    }
}
