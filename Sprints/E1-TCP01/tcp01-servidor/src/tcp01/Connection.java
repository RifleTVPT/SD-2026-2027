package tcp01;

import java.io.*;
import java.net.*;

public class Connection extends Thread {
    ObjectInputStream inObj;
    DataOutputStream out;
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            inObj = new ObjectInputStream(clientSocket.getInputStream());
            out = new DataOutputStream(clientSocket.getOutputStream());
            this.start();
        } catch (IOException e) {
            System.out.println("Connection: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            Object obj = inObj.readObject();

            if (obj instanceof Person) {
                Person p = (Person) obj;
                String responseLocality = p.getPlace().getLocality();
                out.writeUTF(responseLocality);
            } else {
                out.writeUTF("Erro: Objeto desconhecido");
            }

        } catch (InvalidClassException e) {
            System.out.println("InvalidClassException detetada: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println("Class not found: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("close failed: " + e.getMessage());
            }
        }
    }
}