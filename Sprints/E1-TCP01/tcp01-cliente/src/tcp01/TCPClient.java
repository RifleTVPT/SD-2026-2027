package tcp01;

import java.net.*;
import java.io.*;

public class TCPClient {
    public static void main(String args[]) {
        Socket s = null;

        try {
            int serverPort = 7896;

            s = new Socket("localhost", serverPort);

            ObjectOutputStream outObj = new ObjectOutputStream(s.getOutputStream());
            DataInputStream in = new DataInputStream(s.getInputStream());

            Place loc = new Place("3500-001", "Viseu");
            Person p = new Person("Maria", loc, 2004);

            outObj.writeObject(p);
            outObj.flush();

            String data = in.readUTF();
            System.out.println("Received: " + data);

        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    System.out.println("close: " + e.getMessage());
                }
            }
        }
    }
}