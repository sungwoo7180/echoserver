package practice;

public class EnumTest {
    public static void main(String[] args) {
        Color[] colors = new Color[3];
        colors[0] = Color.RED;
        colors[1] = Color.GREEN;
        colors[2] = Color.BLUE;

        for (Color color : colors) {
            System.out.println(color);

        }
    }
}
