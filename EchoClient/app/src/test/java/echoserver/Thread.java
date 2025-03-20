package echoserver;

import java.net.Socket;
import java.text.SimpleDateFormat;

// 클라이언트 하나당 쓰레드 하나를 할당
public class Thread implements Runnable {

    Socket socket;
    private SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");


    public Thread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {

    }
}
