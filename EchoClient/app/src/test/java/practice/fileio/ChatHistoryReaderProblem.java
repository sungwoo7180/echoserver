package practice.fileio;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.charset.CharsetDecoder;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

public class ChatHistoryReaderProblem {

    public static List<String> readLatestChatHistory(String filePath, int numberOfLines) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        long length = raf.length();  // 파일 크기
        List<String> chatHistory = new ArrayList<>();
        int lineCount = 0;

        // CharsetDecoder 사용
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

        // 파일 끝에서부터 한 줄씩 읽기
        long pointer = length - 1;
        StringBuilder sb = new StringBuilder();
        while (pointer >= 0 && lineCount < numberOfLines) {
            raf.seek(pointer);
            byte b = raf.readByte();  // 한 바이트 읽기
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[] {b});  // 바이트 배열로 감싸기
            String decodedString = decodeByteBuffer(byteBuffer, decoder);  // UTF-8로 바이트를 디코딩

            if (decodedString.isEmpty()) {
                pointer--;  // 빈 문자열이 반환되면 다음 바이트를 처리
                continue;
            }

            // 문자가 역순으로 추가됨
            sb.insert(0, decodedString);

            // 한 줄을 역순으로 읽기
            if (decodedString.charAt(0) == '\n' && sb.length() > 0) {
                chatHistory.add(sb.toString());
                sb = new StringBuilder();  // 다음 줄을 읽기 위한 초기화
                lineCount++;
            }

            pointer--;
        }

        // 마지막 한 줄을 추가 (끝에 \n이 없을 수 있기 때문에 추가)
        if (sb.length() > 0) {
            chatHistory.add(sb.toString());
        }

        raf.close();
        return chatHistory;
    }

    private static String decodeByteBuffer(ByteBuffer byteBuffer, CharsetDecoder decoder) throws IOException {
        // ByteBuffer를 디코딩하여 UTF-8로 변환
        CharBuffer charBuffer = CharBuffer.allocate(1024);  // CharBuffer 생성
        decoder.decode(byteBuffer, charBuffer, true);  // ByteBuffer를 CharBuffer로 디코딩
        charBuffer.flip();  // CharBuffer 내용 플립
        return charBuffer.toString();  // CharBuffer에서 String으로 변환
    }

    public static void main(String[] args) throws IOException {
        List<String> latestChatHistory = readLatestChatHistory("app/src/test/java/practice/fileio/chathistory.txt", 20);

        for (String line : latestChatHistory) {
            System.out.println(line);
        }
    }
}
