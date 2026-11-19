import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LAST STAND relay server.
 *
 * A tiny WebSocket server that passes messages between the player who hosts a match
 * and the friends who join it. It runs on a free cloud host (see the deploy guide), so
 * every player only makes a normal outgoing HTTPS connection - which works on school networks.
 *
 * Run locally for testing:  java RelayServer      (listens on port 10000, or $PORT)
 *
 * Messages (binary, first byte = type):
 *   client -> relay : 'H' name            host a room
 *                     'J' code name       join a room
 *                     'D' int target data (host only) send data to one client, 0 = everyone
 *                     'U' data            (client) send data to the host
 *                     'P'                 keep-alive ping
 *   relay -> client : 'R' code            room created
 *                     'O' int id          joined OK, this is your id
 *                     'E' text            error
 *                     'N' int id name     (to host) a friend joined
 *                     'L' int id          (to host) a friend left
 *                     'X'                 (to clients) host left, room closed
 *                     'd' int from data   data (from = 0 means from the host)
 *                     'p'                 pong
 */
public class RelayServer {
    static final int MAX_FRAME = 4 * 1024 * 1024;
    static final int MAX_PER_ROOM = 12;
    static final Map<String, Room> rooms = new ConcurrentHashMap<>();
    static final Random rng = new Random();

    static class Room {
        final String code;
        final Conn host;
        final Map<Integer, Conn> clients = new ConcurrentHashMap<>();
        int nextId = 1;
        Room(String code, Conn host) { this.code = code; this.host = host; }
    }

    public static void main(String[] args) throws IOException {
        int port = 10000;
        String env = System.getenv("PORT");
        if (env != null) try { port = Integer.parseInt(env.trim()); } catch (NumberFormatException ignored) { }
        if (args.length > 0) port = Integer.parseInt(args[0]);
        ServerSocket ss = new ServerSocket(port);
        System.out.println("Last Stand relay listening on port " + port);
        while (true) {
            Socket s = ss.accept();
            Thread t = new Thread(() -> new Conn(s).run(), "conn");
            t.setDaemon(true);
            t.start();
        }
    }

    static String newCode() {
        String letters = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        while (true) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 5; i++) sb.append(letters.charAt(rng.nextInt(letters.length())));
            if (!rooms.containsKey(sb.toString())) return sb.toString();
        }
    }

    // ------------------------------------------------------------------ one connection

    static class Conn {
        final Socket sock;
        InputStream in;
        OutputStream out;
        Room room;
        boolean isHost;
        int id;
        volatile boolean closed;

        Conn(Socket s) { sock = s; }

        void run() {
            try {
                sock.setTcpNoDelay(true);
                sock.setSoTimeout(120_000);
                in = new BufferedInputStream(sock.getInputStream());
                out = new BufferedOutputStream(sock.getOutputStream());
                if (!handshake()) { sock.close(); return; }
                while (!closed) {
                    byte[] msg = readMessage();
                    if (msg == null) break;
                    if (msg.length > 0) handle(msg);
                }
            } catch (Exception ignored) {
            } finally {
                leave();
                try { sock.close(); } catch (IOException ignored) { }
            }
        }

        private String readLine() throws IOException {
            StringBuilder sb = new StringBuilder();
            int c;
            while ((c = in.read()) != -1) {
                if (c == '\n') break;
                if (c != '\r') sb.append((char) c);
                if (sb.length() > 8192) throw new IOException("header too long");
            }
            if (c == -1 && sb.length() == 0) return null;
            return sb.toString();
        }

        private boolean handshake() throws Exception {
            String first = readLine();
            if (first == null) return false;
            Map<String, String> h = new HashMap<>();
            String line;
            while ((line = readLine()) != null && !line.isEmpty()) {
                int i = line.indexOf(':');
                if (i > 0) h.put(line.substring(0, i).trim().toLowerCase(), line.substring(i + 1).trim());
            }
            String key = h.get("sec-websocket-key");
            String upgrade = h.getOrDefault("upgrade", "");
            if (key == null || !upgrade.equalsIgnoreCase("websocket")) {
                // plain web request: health check page (also wakes the server up)
                String body = "Last Stand relay is running. Rooms open: " + rooms.size() + "\n";
                boolean head = first.startsWith("HEAD");
                String resp = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: " + body.length()
                        + "\r\nConnection: close\r\n\r\n" + (head ? "" : body);
                out.write(resp.getBytes(StandardCharsets.US_ASCII));
                out.flush();
                return false;
            }
            MessageDigest sha = MessageDigest.getInstance("SHA-1");
            String accept = Base64.getEncoder().encodeToString(
                    sha.digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII)));
            String resp = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
            out.write(resp.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            return true;
        }

        private int readByte() throws IOException {
            int b = in.read();
            if (b < 0) throw new EOFException();
            return b;
        }

        /** Reads one complete WebSocket message (joining fragments). Returns null on close. */
        private byte[] readMessage() throws IOException {
            ByteArrayOutputStream buf = null;
            while (true) {
                int b0 = readByte(), b1 = readByte();
                boolean fin = (b0 & 0x80) != 0;
                int op = b0 & 0x0F;
                boolean masked = (b1 & 0x80) != 0;
                long len = b1 & 0x7F;
                if (len == 126) len = (readByte() << 8) | readByte();
                else if (len == 127) {
                    len = 0;
                    for (int i = 0; i < 8; i++) len = (len << 8) | readByte();
                }
                if (len > MAX_FRAME) throw new IOException("frame too big");
                byte[] mask = new byte[4];
                if (masked) for (int i = 0; i < 4; i++) mask[i] = (byte) readByte();
                byte[] data = new byte[(int) len];
                int off = 0;
                while (off < len) {
                    int r = in.read(data, off, (int) len - off);
                    if (r < 0) throw new EOFException();
                    off += r;
                }
                if (masked) for (int i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
                if (op == 0x8) { sendFrame(0x8, new byte[0]); return null; }      // close
                if (op == 0x9) { sendFrame(0xA, data); continue; }                // ping -> pong
                if (op == 0xA) continue;                                           // pong
                if (op == 0x1 || op == 0x2) {
                    if (fin) return data;
                    buf = new ByteArrayOutputStream();
                    buf.write(data);
                } else if (op == 0x0 && buf != null) {
                    buf.write(data);
                    if (buf.size() > MAX_FRAME) throw new IOException("message too big");
                    if (fin) return buf.toByteArray();
                }
            }
        }

        synchronized void sendFrame(int op, byte[] data) {
            if (closed) return;
            try {
                out.write(0x80 | op);
                if (data.length < 126) out.write(data.length);
                else if (data.length < 65536) { out.write(126); out.write(data.length >> 8); out.write(data.length & 255); }
                else {
                    out.write(127);
                    for (int i = 7; i >= 0; i--) out.write((int) (((long) data.length >> (8 * i)) & 255));
                }
                out.write(data);
                out.flush();
            } catch (IOException e) {
                closed = true;
            }
        }

        void send(byte[] data) { sendFrame(0x2, data); }

        // ------------------------------------------------------------ protocol

        private void handle(byte[] msg) throws IOException {
            DataInputStream d = new DataInputStream(new ByteArrayInputStream(msg, 1, msg.length - 1));
            switch (msg[0]) {
                case 'P':
                    send(new byte[]{'p'});
                    break;
                case 'H': {
                    if (room != null) return;
                    String code = newCode();
                    room = new Room(code, this);
                    isHost = true;
                    rooms.put(code, room);
                    send(msg("R", code));
                    log("room " + code + " opened");
                    break;
                }
                case 'J': {
                    if (room != null) return;
                    String code = d.readUTF().trim().toUpperCase();
                    String name = d.readUTF();
                    Room r = rooms.get(code);
                    if (r == null) { send(msg("E", "No game with code " + code + ". Check the code and try again.")); return; }
                    synchronized (r) {
                        if (r.clients.size() >= MAX_PER_ROOM) { send(msg("E", "That game is full.")); return; }
                        id = r.nextId++;
                        r.clients.put(id, this);
                    }
                    room = r;
                    ByteArrayOutputStream bo = new ByteArrayOutputStream();
                    DataOutputStream o = new DataOutputStream(bo);
                    o.writeByte('O');
                    o.writeInt(id);
                    send(bo.toByteArray());
                    bo.reset();
                    o.writeByte('N');
                    o.writeInt(id);
                    o.writeUTF(name);
                    r.host.send(bo.toByteArray());
                    break;
                }
                case 'D': {
                    if (room == null || !isHost) return;
                    int target = d.readInt();
                    // forward as 'd' + from(0 = host) + data
                    byte[] fwd = new byte[1 + 4 + (msg.length - 5)];
                    fwd[0] = 'd';
                    System.arraycopy(msg, 5, fwd, 5, msg.length - 5);
                    if (target == 0) { for (Conn c : room.clients.values()) c.send(fwd); }
                    else { Conn c = room.clients.get(target); if (c != null) c.send(fwd); }
                    break;
                }
                case 'U': {
                    if (room == null || isHost) return;
                    byte[] fwd = new byte[1 + 4 + (msg.length - 1)];
                    fwd[0] = 'd';
                    fwd[1] = (byte) (id >> 24); fwd[2] = (byte) (id >> 16); fwd[3] = (byte) (id >> 8); fwd[4] = (byte) id;
                    System.arraycopy(msg, 1, fwd, 5, msg.length - 1);
                    room.host.send(fwd);
                    break;
                }
                default:
                    break;
            }
        }

        private void leave() {
            closed = true;
            Room r = room;
            if (r == null) return;
            room = null;
            if (isHost) {
                rooms.remove(r.code);
                for (Conn c : r.clients.values()) { c.send(new byte[]{'X'}); c.sendFrame(0x8, new byte[0]); }
                log("room " + r.code + " closed");
            } else {
                r.clients.remove(id);
                byte[] m = {'L', (byte) (id >> 24), (byte) (id >> 16), (byte) (id >> 8), (byte) id};
                r.host.send(m);
            }
        }
    }

    static byte[] msg(String type, String text) throws IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream o = new DataOutputStream(bo);
        o.writeByte(type.charAt(0));
        o.writeUTF(text);
        return bo.toByteArray();
    }

    static void log(String s) { System.out.println(new Date() + "  " + s); }
}
