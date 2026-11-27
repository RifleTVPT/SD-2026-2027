import java.io.*;
import java.net.*;

public class UDPServer {

  public static void main(String[] args) {
    DatagramSocket aSocket = null;
    int L = 0;

    try {
      aSocket = new DatagramSocket(6789);
      byte[] buffer = new byte[1000];

      while (true) {
        DatagramPacket request = new DatagramPacket(buffer, buffer.length);
        aSocket.receive(request);

        String mensagem = new String(request.getData(), 0, request.getLength());
        System.out.println("Servidor recebeu: " + mensagem);

        String resposta;

        try {
          int indiceVirgula = mensagem.indexOf(',');
          if (indiceVirgula <= 0 || indiceVirgula == mensagem.length() - 1) {
            throw new IllegalArgumentException("Mensagem mal formada");
          }

          String numeroTexto = mensagem.substring(0, indiceVirgula).trim();
          String texto = mensagem.substring(indiceVirgula + 1).trim();

          if (texto.isEmpty()) {
            throw new IllegalArgumentException("Mensagem sem texto");
          }

          int N = Integer.parseInt(numeroTexto);

          if (N == L + 1) {
            L = N;
            resposta = mensagem;
            System.out.println("Servidor aceita N=" + N + " | L agora = " + L);
          } else {
            resposta = "waitingfor," + (L + 1);
            System.out.println("Servidor rejeita N=" + N + " | espera por " + (L + 1) + " | L atual = " + L);
          }
        } catch (Exception e) {
          resposta = "waitingfor," + (L + 1);
          System.out.println("Servidor: mensagem mal formada. Resposta: " + resposta + " | L atual = " + L);
        }

        InetAddress clientAddress = request.getAddress();
        int clientPort = request.getPort();
        byte[] respostaBytes = resposta.getBytes();

        DatagramPacket reply = new DatagramPacket(respostaBytes, respostaBytes.length, clientAddress, clientPort);
        aSocket.send(reply);
        System.out.println("Servidor envia: " + resposta);
      }

    } catch (SocketException e) {
      System.out.println("Socket: " + e.getMessage());
    } catch (IOException e) {
      System.out.println("IO: " + e.getMessage());
    } finally {
      if (aSocket != null) {
        aSocket.close();
      }
    }
  }
}
