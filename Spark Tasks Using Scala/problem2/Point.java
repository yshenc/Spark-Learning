import java.io.*;
import java.util.Random;


public class Point {

    public String Points() {
        int xlocation = (int)(Math.random()*10000+1);
        int ylocation = (int)(Math.random()*10000+1);
        return  xlocation + "," + ylocation;
    }

    public static void main(String[] Args) throws IOException {
        Point a = new Point();
        String fout = "Points";
        FileOutputStream fos = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        for (int i = 1; i <= 11000000; i++) {
            bw.write(a.Points());
            bw.newLine();
        }
        bw.close();
    }
}



