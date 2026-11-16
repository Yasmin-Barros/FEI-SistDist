package br.fei.sistdist;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class ClientBot {

    private static final String NAME = env("BOT_NAME", "bot-java-1");
    private static final String BROKER = env("BROKER_ADDR", "tcp://broker:5555");
    private static final String PROXY_SUB = env("PROXY_SUB_ADDR", "tcp://proxy:5558");
    private static final int TIMEOUT_MS = Integer.parseInt(env("REQUEST_TIMEOUT_MS", "3000"));

    private static final int MIN_CHANNELS = 5;
    private static final int MIN_SUBSCRIPTIONS = 3;
    private static final int MESSAGES_PER_BURST = 10;
    private static final long PUBLISH_INTERVAL_MS = 1000L;

    private static final List<String> CHANNEL_POOL = List.of(
            "geral", "random", "dev", "noticias", "jogos",
            "musica", "filmes", "ciencia", "esportes", "tecnologia"
    );

    private static final List<String> SAMPLE_MESSAGES = List.of(
            "Olá pessoal, mensagem enviada pelo bot Java!",
            "Alguém online para discutir sistemas distribuídos?",
            "JeroMQ + MessagePack interoperando com Python!",
            "Atualização de status do cliente Java.",
            "Teste automático de publicação no canal.",
            "Verificando entrega via proxy XSUB/XPUB.",
            "Mais uma rodada de mensagens do cliente Java!"
    );

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final ZContext ctx = new ZContext();
    private ZMQ.Socket reqSock;
    private ZMQ.Socket subSock;
    private final Object subLock = new Object();
    private final Set<String> subscribedChannels = ConcurrentHashMap.newKeySet();
    private String user = NAME;
    private long logicalClock = 0L;

    private synchronized long clockSent() { return ++logicalClock; }
    private synchronized void clockReceived(Object remote) {
        if (remote instanceof Number n && n.longValue() >= 0) {
            logicalClock = Math.max(logicalClock, n.longValue());
        }
    }

    public static void main(String[] args) throws Exception {
        new ClientBot().run();
    }

    private void run() throws Exception {
        connectReq();
        startSubscriber();
        info("iniciando | broker=" + BROKER + " | proxy_sub=" + PROXY_SUB);
        doLogin();

        while (true) {
            List<String> channels = syncChannelsAndSubscriptions();
            String targetChannel = channels.get(ThreadLocalRandom.current().nextInt(channels.size()));
            info("iniciando envio de " + MESSAGES_PER_BURST + " mensagens no canal '" + targetChannel + "'");

            for (int i = 1; i <= MESSAGES_PER_BURST; i++) {
                String sample = SAMPLE_MESSAGES.get(ThreadLocalRandom.current().nextInt(SAMPLE_MESSAGES.size()));
                String text = "[" + i + "/" + MESSAGES_PER_BURST + "] " + sample;
                publishMessage(targetChannel, text);
                Thread.sleep(PUBLISH_INTERVAL_MS);
            }
        }
    }

    private void connectReq() {
        if (reqSock != null) {
            reqSock.setLinger(0);
            reqSock.close();
        }
        reqSock = ctx.createSocket(SocketType.REQ);
        reqSock.connect(BROKER);
    }

    private void startSubscriber() {
        subSock = ctx.createSocket(SocketType.SUB);
        subSock.connect(PROXY_SUB);

        Thread subThread = new Thread(() -> {
            ZMQ.Poller poller = ctx.createPoller(1);
            poller.register(subSock, ZMQ.Poller.POLLIN);

            while (!Thread.currentThread().isInterrupted()) {
                ZMsg incoming = null;
                synchronized (subLock) {
                    if (poller.poll(100) > 0) {
                        incoming = ZMsg.recvMsg(subSock);
                    }
                }
                if (incoming != null && incoming.size() >= 2) {
                    double recvTs = nowEpoch();
                    String topic = new String(incoming.pop().getData(), StandardCharsets.UTF_8);
                    byte[] payload = incoming.pop().getData();
                    incoming.destroy();

                    try {
                        Map<String, Object> msg = unpackEnvelope(payload);
                        @SuppressWarnings("unchecked")
                        Map<String, Object> data = (Map<String, Object>) msg.getOrDefault("data", Map.of());
                        clockReceived(msg.get("logical_clock"));
                        double sendTs = toDouble(msg.get("timestamp"));

                        String line = String.format(
                                "[%s] %-10s PUB-RECV  <- #%-10s | autor=%s | msg='%s' | ts_envio=%s | ts_recebimento=%s",
                                nowStr(), user, topic,
                                data.get("user"), data.get("message"),
                                fmtTs(sendTs), fmtTs(recvTs)
                        );
                        System.out.println(line);
                        System.out.flush();
                    } catch (IOException e) {
                        info("erro ao decodificar mensagem PUB/SUB: " + e.getMessage());
                    }
                }
            }
        }, "sub-listener");
        subThread.setDaemon(true);
        subThread.start();
    }

    private void subscribeChannel(String channel) {
        synchronized (subLock) {
            if (subscribedChannels.contains(channel)) {
                return;
            }
            subSock.subscribe(channel.getBytes(StandardCharsets.UTF_8));
            subscribedChannels.add(channel);
        }
        List<String> sorted = new ArrayList<>(subscribedChannels);
        Collections.sort(sorted);
        info("inscrito no canal '" + channel + "' (total inscritos: " + sorted + ")");
    }

    private Map<String, Object> request(String service, Map<String, Object> data) throws IOException {
        double sendTs = nowEpoch();
        byte[] payload = packEnvelope(service, sendTs, data);
        Map<String, Object> sentMsg = unpackEnvelope(payload); // inclui o logical_clock efetivamente serializado

        ZMQ.Poller poller = ctx.createPoller(1);
        poller.register(reqSock, ZMQ.Poller.POLLIN);

        while (true) {
            reqSock.send(payload, 0);
            log("ENVIADA", sentMsg, "broker");

            if (poller.poll(TIMEOUT_MS) > 0) {
                byte[] rawReply = reqSock.recv(0);
                Map<String, Object> reply = unpackEnvelope(rawReply);
                clockReceived(reply.get("logical_clock"));
                log("RECEBIDA", reply, "broker");
                poller.close();
                return reply;
            }
            info("sem resposta em " + TIMEOUT_MS + " ms (serviço indisponível?), tentando de novo...");
            poller.unregister(reqSock);
            connectReq();
            poller.register(reqSock, ZMQ.Poller.POLLIN);
        }
    }

    private void doLogin() throws Exception {
        int attempt = 0;
        while (true) {
            Map<String, Object> reply = request("login", Map.of("user", user));
            Map<String, Object> data = getData(reply);
            if ("sucesso".equals(data.get("status"))) {
                info("login OK no " + data.get("server"));
                return;
            }
            attempt++;
            info("login recusado: " + data.get("description"));
            String sanitized = user.replaceAll("[^A-Za-z0-9_.-]", "-");
            if (sanitized.length() > 32) sanitized = sanitized.substring(0, 32);
            if (sanitized.isEmpty()) sanitized = "bot-" + ThreadLocalRandom.current().nextInt(1000, 9999);

            user = !sanitized.equals(user)
                    ? sanitized
                    : sanitized.substring(0, Math.min(26, sanitized.length())) + "-" + ThreadLocalRandom.current().nextInt(100, 999);
            info("tentando login novamente como '" + user + "' (tentativa " + (attempt + 1) + ")");
            Thread.sleep(1000);
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> listChannels() throws IOException {
        Map<String, Object> reply = request("channels", Map.of("user", user));
        Map<String, Object> data = getData(reply);
        if (!"sucesso".equals(data.get("status"))) {
            info("erro ao listar canais: " + data.get("description"));
            return new ArrayList<>();
        }
        Object rawList = data.get("channels");
        List<String> result = new ArrayList<>();
        if (rawList instanceof List<?> list) {
            for (Object item : list) {
                result.add(String.valueOf(item));
            }
        }
        return result;
    }

    private boolean createChannel(String name) throws IOException {
        Map<String, Object> reply = request("channel", Map.of("user", user, "channel", name));
        Map<String, Object> data = getData(reply);
        if ("sucesso".equals(data.get("status"))) {
            info("canal '" + name + "' criado");
            return true;
        }
        info("não foi possível criar '" + name + "': " + data.get("description"));
        return false;
    }

    private void publishMessage(String channel, String text) throws IOException {
        Map<String, Object> reply = request("publish", Map.of(
                "user", user,
                "channel", channel,
                "message", text
        ));
        Map<String, Object> data = getData(reply);
        if (!"sucesso".equals(data.get("status"))) {
            info("erro ao publicar em '" + channel + "': " + data.get("description"));
        }
    }

    private List<String> syncChannelsAndSubscriptions() throws IOException {
        List<String> channels = listChannels();

        if (channels.size() < MIN_CHANNELS) {
            List<String> candidates = new ArrayList<>();
            for (String c : CHANNEL_POOL) {
                if (!channels.contains(c)) candidates.add(c);
            }
            if (!candidates.isEmpty()) {
                String chosen = candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
                if (createChannel(chosen) && !channels.contains(chosen)) {
                    channels.add(chosen);
                }
            }
        }

        if (channels.isEmpty() && createChannel("geral")) {
            channels.add("geral");
        }

        if (subscribedChannels.size() < MIN_SUBSCRIPTIONS) {
            List<String> unsubscribed = new ArrayList<>();
            for (String c : channels) {
                if (!subscribedChannels.contains(c)) unsubscribed.add(c);
            }
            if (!unsubscribed.isEmpty()) {
                subscribeChannel(unsubscribed.get(ThreadLocalRandom.current().nextInt(unsubscribed.size())));
            }
        }

        return channels;
    }

    private byte[] packEnvelope(String service, double timestamp, Map<String, Object> data) throws IOException {
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(4);

            packer.packString("service");
            packer.packString(service);

            packer.packString("timestamp");
            packer.packDouble(timestamp);

            packer.packString("logical_clock");
            packer.packLong(clockSent());

            packer.packString("data");
            packer.packMapHeader(data.size());
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                packer.packString(entry.getKey());
                packDynamicValue(packer, entry.getValue());
            }
            return packer.toByteArray();
        }
    }

    private void packDynamicValue(MessageBufferPacker packer, Object val) throws IOException {
        if (val == null) {
            packer.packNil();
        } else if (val instanceof String s) {
            packer.packString(s);
        } else if (val instanceof Integer i) {
            packer.packInt(i);
        } else if (val instanceof Long l) {
            packer.packLong(l);
        } else if (val instanceof Double d) {
            packer.packDouble(d);
        } else if (val instanceof Boolean b) {
            packer.packBoolean(b);
        } else {
            packer.packString(val.toString());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> unpackEnvelope(byte[] raw) throws IOException {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(raw)) {
            Value v = unpacker.unpackValue();
            Object converted = convertValue(v);
            if (converted instanceof Map<?, ?> m) {
                return (Map<String, Object>) m;
            }
            return Map.of();
        }
    }

    private Object convertValue(Value v) {
        if (v.isNilValue()) return null;
        if (v.isBooleanValue()) return v.asBooleanValue().getBoolean();
        if (v.isIntegerValue()) return v.asIntegerValue().toLong();
        if (v.isFloatValue()) return v.asFloatValue().toDouble();
        if (v.isStringValue()) return v.asStringValue().asString();
        if (v.isArrayValue()) {
            List<Object> list = new ArrayList<>();
            for (Value item : v.asArrayValue()) {
                list.add(convertValue(item));
            }
            return list;
        }
        if (v.isMapValue()) {
            Map<String, Object> map = new LinkedHashMap<>();
            MapValue mv = v.asMapValue();
            for (Map.Entry<Value, Value> e : mv.entrySet()) {
                map.put(String.valueOf(convertValue(e.getKey())), convertValue(e.getValue()));
            }
            return map;
        }
        return v.toString();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getData(Map<String, Object> msg) {
        Object d = msg.get("data");
        if (d instanceof Map<?, ?> m) {
            return (Map<String, Object>) m;
        }
        return Map.of();
    }

    private void log(String direction, Map<String, Object> msg, String peer) {
        String arrow = "ENVIADA".equals(direction) ? "->" : "<-";
        double ts = toDouble(msg.get("timestamp"));
        String line = String.format(
                "[%s] %-10s %-9s %s %-8s | service=%s | ts_envio=%s | logical_clock=%s | data=%s",
                nowStr(), user, direction, arrow, peer,
                msg.get("service"), fmtTs(ts), msg.get("logical_clock"), msg.get("data")
        );
        System.out.println(line);
        System.out.flush();
    }

    private void info(String text) {
        System.out.printf("[%s] %-10s %s%n", nowStr(), user, text);
        System.out.flush();
    }

    private static double nowEpoch() {
        return System.currentTimeMillis() / 1000.0;
    }

    private static String nowStr() {
        return LocalTime.now().format(TIME_FMT);
    }

    private static String fmtTs(double epochSeconds) {
        if (epochSeconds <= 0) return "??";
        Instant inst = Instant.ofEpochMilli((long) (epochSeconds * 1000));
        return inst.atZone(ZoneId.systemDefault()).toLocalTime().format(TIME_FMT);
    }

    private static double toDouble(Object o) {
        if (o instanceof Number n) return n.doubleValue();
        return 0.0;
    }

    private static String env(String key, String def) {
        String val = System.getenv(key);
        return (val == null || val.isBlank()) ? def : val;
    }
}
