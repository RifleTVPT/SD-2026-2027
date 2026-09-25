import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDPServer {

  private static final List<String> listaRececao = new ArrayList<>();
  private static final Map<Integer, String> mensagensTemporarias = new HashMap<>();

  public static void main(String[] args) {
    DatagramSocket aSocket = null;
    int L = 0;

    try {
      aSocket = new DatagramSocket(6789);
      byte[] buffer = new byte[1000];

      while (true) {
        DatagramPacket request = new DatagramPacket(buffer, buffer.length);
        aSocket.receive(request);

        String mensagem = new String(request.getData(), 0, request.getLength()).trim();
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
          int antigaL = L;
          int novaL = processDeliveredMessages(L, N, texto);

          if (novaL != antigaL) {
            L = novaL;
            resposta = N + "," + texto;
          } else {
            resposta = "waitingfor," + (L + 1);
          }

        } catch (Exception e) {
          resposta = "waitingfor," + (L + 1);
        }

        System.out.println("Recebi agora mensagem " + mensagem + " | L=" + L + " | temporarias=" + mensagensTemporarias + " | entregues=" + listaRececao);

        InetAddress clientAddress = request.getAddress();
        int clientPort = request.getPort();
        byte[] respostaBytes = resposta.getBytes();

        DatagramPacket reply = new DatagramPacket(respostaBytes, respostaBytes.length, clientAddress, clientPort);
        aSocket.send(reply);
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

  public static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage) {
    if (nCurrentMessage <= nLastMessageInOrder) {
      return nLastMessageInOrder;
    }

    if (nCurrentMessage == nLastMessageInOrder + 1) {
      listaRececao.add(nCurrentMessage + "," + currentMessage);
      nLastMessageInOrder = nCurrentMessage;

      while (mensagensTemporarias.containsKey(nLastMessageInOrder + 1)) {
        int proximaMensagem = nLastMessageInOrder + 1;
        String mensagem = mensagensTemporarias.remove(proximaMensagem);
        listaRececao.add(proximaMensagem + "," + mensagem);
        nLastMessageInOrder = proximaMensagem;
      }

      return nLastMessageInOrder;
    }

    if (!mensagensTemporarias.containsKey(nCurrentMessage)) {
      mensagensTemporarias.put(nCurrentMessage, currentMessage);
    }

    return nLastMessageInOrder;
  }
}
