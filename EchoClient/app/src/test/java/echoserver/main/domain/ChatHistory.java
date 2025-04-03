package echoserver.main.domain;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter @Setter
public class ChatHistory {
    String nickname;
    String message;
    Date date;

    @Override
    public String toString() {
        return "[" + nickname  + "] :  " + message + " [" + date + "]";
    }

    public ChatHistory(String nickname, String message, Date date) {
        this.nickname = nickname;
        this.message = message;
        this.date = date;
    }
}


