package echoserver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.Scanner;

public class EchoClient {

    private static final int MAX_BYTES = 140; // 바이트 크기 제한

    public static void main(String[] args) {
        try (
                // Socket socket = new Socket("192.168.30.36", 12346);
                Socket socket = new Socket("localhost", 12346);

                Scanner scanner = new Scanner(System.in);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)
        ) {
            System.out.println("Connected to server");

            while (true) {
                String line = getUserInput(scanner);

                writer.println(line);
                if ("quit".equalsIgnoreCase(line)) {
                    break;
                }
                System.out.println("Received: " + reader.readLine());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 사용자 입력을 받아 검증하는 메서드
    private static String getUserInput(Scanner scanner) {
        String line = null;
        boolean validInput = false;

        while (!validInput) {
            System.out.print("Enter text: ");
            line = scanner.nextLine();
            validInput = validateInput(line);
        }
        return line;
    }

    // 입력 검증 함수 (isEmpty + isOverMaxBytes 통합)
    private static boolean validateInput(String line) {
        if (isEmpty(line)) {
            System.out.println("메시지는 공백일 수 없습니다.");
            return false;
        }
        if (isOverMaxBytes(line)) {
            System.out.println("메시지가 너무 깁니다. " + MAX_BYTES + "바이트 이내로 입력하세요.");
            return false;
        }
        return true;
    }

    // 공백 혹은 빈 문자열 방지
    private static boolean isEmpty(String line) {
        return line.isEmpty();
    }

    // 메시지의 바이트 크기 검사
    private static boolean isOverMaxBytes(String line) {
        try {
            return line.getBytes("UTF-8").length > MAX_BYTES;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return true;
        }
    }
}