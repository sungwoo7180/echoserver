package echoserver.main.domain;

// TODO 리눅스환경에서 배포할때 어떻게 처리 해야 할지 고민
// gradle 인 경우 Maven 인 경우 쌩 이클립스 프로젝트인 경우
// Library 같은 경우에는 추가해주면 될 것 같은데
// import lombok.Getter;
// import lombok.Setter;



import java.nio.ByteBuffer;

// @Getter
// @Setter
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
