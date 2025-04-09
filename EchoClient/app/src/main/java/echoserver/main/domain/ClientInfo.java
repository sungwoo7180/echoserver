package echoserver.main.domain;

import java.nio.ByteBuffer;

public class ClientInfo {
    private final ByteBuffer buffer;
    private String nickname;
    private boolean registered;

    public ClientInfo(int bufferSize) {
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.registered = false;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public boolean isRegistered() {
        return registered;
    }

    public void setRegistered(boolean registered) {
        this.registered = registered;
    }
}
