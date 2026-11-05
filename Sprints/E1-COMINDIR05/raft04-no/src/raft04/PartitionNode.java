package raft04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class PartitionNode {

    private final int k;
    private final int i;
    private final int N;
    private final String api;
    private final String host = "127.0.0.1";

    private final RaftState state;
    private final RecordFile file;
    private final Partitioner partitioner;
    private final IpfsPubSub ipfs;

    private long lastSeq = 0;
    private long lastTerm = 0;
    private long lamport = 0;
    private VectorClock vector;

    private long hb = 0;
    private final List<SensorRecord> pendentes = new ArrayList<>();
    private ScheduledExecutorService leaderScheduler;

    public PartitionNode(int k, int i, int N, String api) throws IOException {
        this.k = k;
        this.i = i;
        this.N = N;
        this.api = api;
        this.ipfs = new IpfsPubSub(api);
        this.partitioner = new Partitioner(N);

        this.file = new RecordFile("no-p" + k + "-n" + i + ".txt");
        SensorRecord lr = file.last(N);
        if (lr != null) {
            this.lastSeq = lr.getSeq();
            this.lastTerm = lr.getTerm();
            this.lamport = lr.getLamport();
            this.vector = lr.getVector().copy();
        } else {
            this.vector = new VectorClock(N);
        }

        Path stateFile = Paths.get("estado-p" + k + "-n" + i + ".txt");
        this.state = new RaftState(k, i, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS, stateFile, this::logEvent);
    }

    private void logEvent(String evt) {
        String line = System.currentTimeMillis() + ";" + evt;
        System.out.println(line);
        try {
            Files.writeString(Paths.get("eventos-p" + k + "-n" + i + ".txt"),
                    line + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException ignored) {}
    }

    public void start() {
        startTcpServer();

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            if (Wire.isolated(k, i)) return;
            long cTerm = state.tick();
            if (cTerm != -1) {
                synchronized (this) {
                    VoteRequest req = new VoteRequest(cTerm, i, lastSeq, lastTerm);
                    try { ipfs.publish(Wire.topicElection(k), req.encode()); } catch (Exception ignored) {}
                }
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        new Thread(() -> {
            try {
                ipfs.subscribe(Wire.topicElection(k), msg -> {
                    if (Wire.isolated(k, i)) return;
                    String type = Wire.type(msg);
                    if ("PEDIDO_VOTO".equals(type)) {
                        VoteRequest req = VoteRequest.decode(msg);
                        if (req.getCandidate() == i) return; 
                        RaftState.Vote v;
                        synchronized (this) {
                            v = state.onVoteRequest(req.getTerm(), req.getCandidate(), req.getLastSeq(), req.getLastTerm(), lastSeq, lastTerm);
                        }
                        VoteReply rep = new VoteReply(v.term, i, req.getCandidate(), v.granted);
                        try { ipfs.publish(Wire.topicElection(k), rep.encode()); } catch (Exception ignored) {}
                    } else if ("VOTO".equals(type)) {
                        VoteReply rep = VoteReply.decode(msg);
                        if (rep.getVoter() == i) return; 
                        boolean becameLeader;
                        synchronized (this) {
                            becameLeader = state.onVote(rep.getTerm(), rep.getVoter(), rep.getCandidate(), rep.isGranted());
                            if (becameLeader) onBecomeLeader();
                        }
                    }
                });
            } catch (Exception ignored) {}
        }).start();

        new Thread(() -> {
            try {
                ipfs.subscribe(Wire.topicData(k), msg -> {
                    if (Wire.isolated(k, i)) return;
                    if ("LOTE".equals(Wire.type(msg))) {
                        Batch b = Batch.decode(msg, N);
                        if (b.getLeader() == i) return; 
                        boolean apply = state.onBatch(b.getTerm(), b.getLeader());
                        if (!apply) {
                            logEvent("IGNORADO;p" + k + ";termo=" + b.getTerm());
                            return;
                        }
                        logEvent("RECEBIDO;p" + k + ";termo=" + b.getTerm() + ";lider=n" + b.getLeader());
                        processBatchAsFollower(b);
                    }
                });
            } catch (Exception ignored) {}
        }).start();

        new Thread(() -> {
            try {
                ipfs.subscribe(Wire.topicIn(k), msg -> {
                    if (!"IN".equals(Wire.type(msg))) return;
                    String[] f = Wire.body(msg).split(";");
                    if (f.length != 4) return;
                    String sensor = f[0];
                    double temp = Double.parseDouble(f[1]);
                    long lMsg = Long.parseLong(f[2]);
                    VectorClock vMsg = VectorClock.parse(f[3], N);

                    if (partitioner.of(sensor) != k) return;

                    synchronized (this) {
                        long t = state.termIfLeader();
                        if (t == -1) {
                            logEvent("IGNORADO;p" + k + ";leitura=" + sensor);
                            return;
                        }
                        lamport = Math.max(lamport, lMsg) + 1;
                        vector.merge(vMsg);
                        vector.tick(k);
                        lastSeq++;
                        lastTerm = t;

                        SensorRecord r = SensorRecord.now(lastSeq, t, sensor, temp, lamport, vector.copy());
                        try {
                            file.append(r);
                        } catch (IOException e) {
                            logEvent("ERRO;p" + k + ";erro_disco=" + e.getMessage());
                        }
                        pendentes.add(r);
                    }
                });
            } catch (Exception ignored) {}
        }).start();
    }

    private synchronized void onBecomeLeader() {
        hb = 0;
        pendentes.clear();
        try {
            SensorRecord lr = file.last(N);
            if (lr != null) {
                lastSeq = lr.getSeq();
                lastTerm = lr.getTerm();
                lamport = lr.getLamport();
                vector = lr.getVector().copy();
            } else {
                lastSeq = 0;
                lastTerm = 0;
                lamport = 0;
                vector = new VectorClock(N);
            }
        } catch (IOException ignored) {}
        if (leaderScheduler != null) leaderScheduler.shutdownNow();
        leaderScheduler = Executors.newSingleThreadScheduledExecutor();
        leaderScheduler.scheduleAtFixedRate(this::enviarLote, 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    private void enviarLote() {
        long t;
        List<SensorRecord> aEnviar;
        long seqAtual, termAtual, hbAtual;

        synchronized (this) {
            t = state.termIfLeader();
            if (t == -1) {
                if (leaderScheduler != null) leaderScheduler.shutdown();
                return;
            }
            aEnviar = new ArrayList<>(pendentes);
            pendentes.clear();
            seqAtual = lastSeq;
            termAtual = lastTerm;
            hb++;
            hbAtual = hb;
        }

        Batch b = new Batch(t, i, host, Wire.nodePort(k, i), hbAtual, seqAtual, termAtual, aEnviar);
        String enc = b.encode();

        if (!Wire.isolated(k, i)) {
            try {
                ipfs.publish(Wire.topicData(k), enc);
                String line = t + ";" + hbAtual + ";" + System.currentTimeMillis() + ";" + seqAtual + ";" + termAtual + ";" + aEnviar.size() + ";" + enc.getBytes(StandardCharsets.UTF_8).length + "\n";
                Files.writeString(Paths.get("hb-p" + k + "-n" + i + ".txt"), line, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (Exception ignored) {}
        }
    }

    private synchronized void processBatchAsFollower(Batch b) {
        try {
            List<SensorRecord> local = file.readAll(N);
            Map<Long, SensorRecord> map = new HashMap<>();
            for (SensorRecord r : local) map.put(r.getSeq(), r);

            boolean hasDivergence = false;
            for (SensorRecord r : b.getRecords()) {
                if (map.containsKey(r.getSeq())) {
                    SensorRecord old = map.get(r.getSeq());
                    if (old.getTerm() != r.getTerm()) {
                        hasDivergence = true;
                        break;
                    }
                } else if (r.getSeq() == lastSeq + 1) {
                    file.append(r);
                    lastSeq = r.getSeq();
                    lastTerm = r.getTerm();
                    lamport = Math.max(lamport, r.getLamport());
                    vector.merge(r.getVector());
                    map.put(r.getSeq(), r);
                }
            }

            if (map.containsKey(b.getLastSeq()) && map.get(b.getLastSeq()).getTerm() != b.getLastTerm()) {
                hasDivergence = true;
            }
            if (lastSeq > b.getLastSeq()) {
                hasDivergence = true;
            }

            if (hasDivergence) {
                resolveDivergenceViaTcp(b.getAddress(), b.getPort(), b.getLastSeq());
            } else if (lastSeq < b.getLastSeq()) {
                recoverGapsViaTcp(b.getAddress(), b.getPort(), lastSeq + 1, b.getLastSeq());
            }
        } catch (IOException ignored) {}
    }

    private void recoverGapsViaTcp(String host, int port, long de, long ate) {
        if (Wire.isolated(k, i)) return;
        try (Socket s = new Socket(host, port);
             PrintWriter out = new PrintWriter(s.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
            out.println("PEDIDO " + de + " " + ate);
            String line;
            while ((line = in.readLine()) != null && !"FIM".equals(line)) {
                SensorRecord r = SensorRecord.fromLine(line, N);
                synchronized (this) {
                    if (r.getSeq() == lastSeq + 1) {
                        file.append(r);
                        lastSeq = r.getSeq();
                        lastTerm = r.getTerm();
                        lamport = Math.max(lamport, r.getLamport());
                        vector.merge(r.getVector());
                    }
                }
            }
            logEvent("RECUPERACAO;p" + k + ";de=" + de + ";ate=" + ate);
        } catch (Exception ignored) {}
    }

    private void resolveDivergenceViaTcp(String host, int port, long leaderLastSeq) {
        if (Wire.isolated(k, i)) return;
        try {
            List<SensorRecord> local = file.readAll(N);
            if (local.isEmpty()) {
                recoverGapsViaTcp(host, port, 1, leaderLastSeq);
                return;
            }

            List<Integer> blockStartIndices = new ArrayList<>();
            for (int idx = 0; idx < local.size(); idx++) {
                if (idx == 0 || local.get(idx).getTerm() != local.get(idx - 1).getTerm()) {
                    blockStartIndices.add(idx);
                }
            }

            long divergentSeq = 1;
            List<SensorRecord> fetchedFromLeader = new ArrayList<>();

            for (int bIdx = blockStartIndices.size() - 1; bIdx >= 0; bIdx--) {
                int startIdx = blockStartIndices.get(bIdx);
                long blockFirstSeq = local.get(startIdx).getSeq();

                List<SensorRecord> attempt = new ArrayList<>();
                try (Socket s = new Socket(host, port);
                     PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                     BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
                    out.println("PEDIDO " + blockFirstSeq + " " + leaderLastSeq);
                    String line;
                    while ((line = in.readLine()) != null && !"FIM".equals(line)) {
                        attempt.add(SensorRecord.fromLine(line, N));
                    }
                }

                if (!attempt.isEmpty() && attempt.get(0).getSeq() == blockFirstSeq
                        && attempt.get(0).getTerm() == local.get(startIdx).getTerm()) {

                    divergentSeq = blockFirstSeq;
                    fetchedFromLeader = attempt;

                    Map<Long, SensorRecord> attemptMap = new HashMap<>();
                    for (SensorRecord ar : attempt) attemptMap.put(ar.getSeq(), ar);

                    for (int curIdx = startIdx; curIdx < local.size(); curIdx++) {
                        SensorRecord locR = local.get(curIdx);
                        SensorRecord attR = attemptMap.get(locR.getSeq());
                        if (attR != null && attR.getTerm() == locR.getTerm()) {
                            divergentSeq = locR.getSeq() + 1;
                        } else {
                            divergentSeq = locR.getSeq();
                            break;
                        }
                    }
                    break;
                }
            }

            List<SensorRecord> truncatedList = new ArrayList<>();
            List<SensorRecord> discarded = new ArrayList<>();
            Set<Long> discardedTerms = new LinkedHashSet<>();

            for (SensorRecord r : local) {
                if (r.getSeq() < divergentSeq) {
                    truncatedList.add(r);
                } else {
                    discarded.add(r);
                    discardedTerms.add(r.getTerm());
                }
            }

            for (SensorRecord r : fetchedFromLeader) {
                if (r.getSeq() >= divergentSeq) {
                    truncatedList.add(r);
                }
            }

            synchronized (this) {
                file.rewriteAll(truncatedList);
                SensorRecord last = file.last(N);
                if (last != null) {
                    lastSeq = last.getSeq();
                    lastTerm = last.getTerm();
                    lamport = last.getLamport();
                    vector = last.getVector().copy();
                } else {
                    lastSeq = 0;
                    lastTerm = 0;
                    lamport = 0;
                    vector = new VectorClock(N);
                }
            }

            logEvent("TRUNCADO;p" + k + ";de=" + divergentSeq + ";descartados=" + discarded.size() + ";termos=" + discardedTerms);

        } catch (Exception ignored) {}
    }

    private void startTcpServer() {
        new Thread(() -> {
            try (ServerSocket server = new ServerSocket(Wire.nodePort(k, i))) {
                while (true) {
                    Socket s = server.accept();
                    if (Wire.isolated(k, i)) {
                        try { s.close(); } catch (Exception ignored) {}
                        continue;
                    }
                    new Thread(() -> {
                        try (BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                             PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                             Socket sock = s) {
                            if (Wire.isolated(k, i)) return;
                            String req = in.readLine();
                            if (req != null && req.startsWith("PEDIDO ")) {
                                String[] parts = req.split(" ");
                                long de = Long.parseLong(parts[1]);
                                long ate = Long.parseLong(parts[2]);
                                List<SensorRecord> list = file.range(de, ate, N);
                                for (SensorRecord r : list) {
                                    if (Wire.isolated(k, i)) return;
                                    out.println(r.toLine());
                                }
                                out.println("FIM");
                            }
                        } catch (Exception ignored) {}
                    }).start();
                }
            } catch (Exception ignored) {}
        }).start();
    }

    public static void main(String[] args) throws Exception {
        int k = args.length > 0 ? Integer.parseInt(args[0]) : 0;
        int i = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        int N = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        String api = "http://127.0.0.1:5001/api/v0";

        PartitionNode node = new PartitionNode(k, i, N, api);
        node.start();
        System.out.println("PartitionNode p" + k + " n" + i + " a correr...");
    }
}
