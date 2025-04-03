package echoserver.main.domain;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

@Getter
@Setter
public class ClientInfo {
    private String nickname;
    private boolean registered;
    private final ByteBuffer buffer;

    public ClientInfo(int bufferSize) {
        this.buffer = ByteBuffer.allocate(bufferSize);
        this.registered = false;
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

    public ByteBuffer getBuffer() {
        return buffer;
    }
}
