import java.io.*;
import java.net.*;
import java.util.Scanner;

public class UDPClient {

  public static void main(String[] args) {
    DatagramSocket aSocket = null;
    Scanner scanner = new Scanner(System.in);
    int nextSequence = 1;

    try {
      aSocket = new DatagramSocket();
      InetAddress aHost = InetAddress.getByName("localhost");
      int serverPort = 6789;

      System.out.println("Escolha o modo:");
      System.out.println("1 - automatico");
      System.out.println("2 - manual");
      System.out.print("Opcao: ");

      String modoTexto = scanner.nextLine().trim();
      int modo;

      try {
        modo = Integer.parseInt(modoTexto);
      } catch (NumberFormatException e) {
        System.out.println("Modo invalido. Usa 1 ou 2.");
        return;
      }

      while (true) {
        System.out.print("Mensagem (ou 'sair' para terminar): ");
        String texto = scanner.nextLine();

        if (texto.equalsIgnoreCase("sair")) {
          break;
        }

        int sequence;
        if (modo == 1) {
          sequence = nextSequence;
          nextSequence++;
        } else if (modo == 2) {
          System.out.print("Numero de sequencia: ");
          String seqTexto = scanner.nextLine().trim();
          try {
            sequence = Integer.parseInt(seqTexto);
          } catch (NumberFormatException e) {
            System.out.println("Numero invalido. Tente de novo.");
            continue;
          }
        } else {
          System.out.println("Modo invalido. Usa 1 ou 2.");
          return;
        }

        String payload = sequence + "," + texto;
        byte[] m = payload.getBytes();

        DatagramPacket request = new DatagramPacket(m, m.length, aHost, serverPort);
        aSocket.send(request);

        byte[] buffer = new byte[1000];
        DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
        aSocket.receive(reply);

        String resposta = new String(reply.getData(), 0, reply.getLength());
        System.out.println("Mensagem enviada: " + payload);

        if (resposta.startsWith("waitingfor")) {
          System.out.println("Resposta: " + resposta + "  [pedido de retransmissao]");
        } else {
          System.out.println("Resposta: " + resposta + "  [echo]");
        }
      }

    } catch (SocketException e) {
      System.out.println("Socket: " + e.getMessage());
    } catch (IOException e) {
      System.out.println("IO: " + e.getMessage());
    } finally {
      if (aSocket != null) {
        aSocket.close();
      }
      scanner.close();
    }
  }
}
