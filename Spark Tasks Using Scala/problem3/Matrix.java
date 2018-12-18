import java.io.*;
import java.util.Random;

public class Matrix {
    public static void main(String args[]) throws IOException{
        FileOutputStream fs1 = new FileOutputStream(new File("/home/mqp/Desktop/M1.txt"));
        FileOutputStream fs2 = new FileOutputStream(new File("/home/mqp/Desktop/M2.txt"));
        PrintStream p1 = new PrintStream(fs1);
        PrintStream p2 = new PrintStream(fs2);
        for (int x = 1; x <= 1000; x++) {
            for (int y = 1; y <= 1000; y++){
                int value1 = (int)(Math.random()*100);
                int value2 = (int)(Math.random()*100);
                p1.print(x+","+y+","+value1);
                p1.print("\n");
                p2.print(x+","+y+","+value2);
                p2.print("\n");
            }
        }
   }
}



