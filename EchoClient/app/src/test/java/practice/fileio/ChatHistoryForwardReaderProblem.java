package practice.fileio;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ChatHistoryForwardReaderProblem {

    public static List<String> readLatestChatHistory(String filePath, int numberOfLines) throws IOException {
        List<String> chatHistory = new ArrayList<>();
        int lineCount = 0;

        // InputStreamReader 와 BufferedReader 를 사용하여 정방향으로 파일을 읽음
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null && lineCount < numberOfLines) {
                chatHistory.add(line);
                lineCount++;
            }
        }

        return chatHistory;
    }

    public static void main(String[] args) throws IOException {
        List<String> latestChatHistory = readLatestChatHistory("app/src/test/java/practice/fileio/chathistory.txt", 20);

        // 읽은 채팅 내역 출력
        for (String line : latestChatHistory) {
            System.out.println(line);
        }
    }
}
