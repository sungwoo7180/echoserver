package echoserver.util;


import static echoserver.util.MyLogger.log;

public class MyLoggerMain {

    public static void main(String[] args) {
        log("hello thread");
        log(123);
    }
}
