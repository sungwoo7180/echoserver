package practice.fileio;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class FileIOTestImpl {

    public static void main(String[] args) {

        try (FileOutputStream fileOutputStream = new FileOutputStream("app/src/test/java/practice/fileio/chathistory.txt", true);
             FileInputStream fileInputStream = new FileInputStream("app/src/test/java/practice/fileio/chathistory.txt")
        ) {

            // 데이터를 기록하는 부분
            ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(ZoneId.systemDefault());   // 현재 시간대
            Date currentDate = Date.from(zonedDateTime.toInstant());                            // ZonedDateTime 을 Date 객체로 변환

            // 데이터 생성
            ChatHistory a = new ChatHistory("sungwoo", "안녕하세요", currentDate);
            ChatHistory b = new ChatHistory("qwer", "난 안녕 못하는데요", currentDate);
            ChatHistory c = new ChatHistory("ㅁㄴㅇㄹ", "뷁", currentDate);

            // 기록
            recordChatHistory(fileOutputStream, a);
            recordChatHistory(fileOutputStream, b);
            recordChatHistory(fileOutputStream, c);

            List<String> latestChatHistory = readChatHistoryReversed("app/src/test/java/practice/fileio/chathistory.txt", 20);
            for (String line : latestChatHistory) {
                System.out.println(line);
            }

            // 읽기
//            List<ChatHistory> chathistoryList = readChatHistory(fileInputStream);
//            for (ChatHistory chatHistory : chathistoryList) {
//                System.out.println(chatHistory.toString());
//            }

        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("IO Exception");
        }
    }

    // 데이터를 기록하는 메소드
    public static void recordChatHistory(FileOutputStream fileOutputStream, ChatHistory history) {
        try {
            String historyString = history.toString() + "\n";
            byte[] bytes = historyString.getBytes(StandardCharsets.UTF_8);
            fileOutputStream.write(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 채팅 기록을 읽어오는 메소드
    public static List<ChatHistory> readChatHistory(FileInputStream fileInputStream) throws IOException {
        List<ChatHistory> list = new ArrayList<>();
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(inputStreamReader);

        String line;
        while ((line = reader.readLine()) != null) {
            // 한 줄씩 읽어서 ChatHistory 로 변환하여 리스트에 추가
            String[] parts = line.split(" : ");
            if (parts.length == 2) {
                String user = parts[0].trim();
                String message = parts[1].trim();
                // 시간 처리
                Date currentDate = new Date(); // 여기서 시간 파싱을 추가해야 할 수 있습니다.
                list.add(new ChatHistory(user, message, currentDate));
            }
        }

        return list;
    }

    public static List<String> readChatHistoryReversed(String filePath, int numberOfLines) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath, "r");
        long pointer = raf.length() - 1;    // 파일 크기 - 1 = 파일
        List<String> result = new ArrayList<>();
        ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream();

        while (pointer >= 0 && result.size() < numberOfLines) {
            raf.seek(pointer);
            byte b = raf.readByte();

            // 라인 종료 문자(\n) 확인
            if (b == '\n') {
                // 현재까지 모은 바이트는 거꾸로 되어 있으므로 뒤집기
                byte[] lineBytes = reverseBytes(lineBuffer.toByteArray());
                String line = new String(lineBytes, StandardCharsets.UTF_8);
                result.add(line.trim());
                lineBuffer.reset();  // 다음 줄을 위해 초기화
            } else {
                lineBuffer.write(b);  // 거꾸로 읽기
            }

            pointer--;
        }

        // 파일 시작이면서 아직 라인이 남아있을 수 있으니 마지막 줄 처리
        if (lineBuffer.size() > 0) {
            byte[] lineBytes = reverseBytes(lineBuffer.toByteArray());
            String line = new String(lineBytes, StandardCharsets.UTF_8);
            result.add(line.trim());
        }

        raf.close();
        return result;
    }

    private static byte[] reverseBytes(byte[] input) {
        byte[] reversed = new byte[input.length];
        for (int i = 0; i < input.length; i++) {
            reversed[i] = input[input.length - 1 - i];
        }
        return reversed;
    }
}
