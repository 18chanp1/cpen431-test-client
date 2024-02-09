package com.s82033788.CPEN431.A4T.wrappers;

import com.s82033788.CPEN431.A4T.newProto.*;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.zip.CRC32;

public class KVClient implements Callable<Boolean> {
    private InetAddress serverAddress;
    private int serverPort;
    private DatagramSocket socket;
    byte[] publicBuf;

    /* Request Codes */
    public final static int REQ_CODE_PUT = 0x01;
    public final static int REQ_CODE_GET = 0X02;
    public final static int REQ_CODE_DEL = 0X03;
    public final static int REQ_CODE_SHU = 0X04;
    public final static int REQ_CODE_WIP = 0X05;
    public final static int REQ_CODE_ALI = 0X06;
    public final static int REQ_CODE_PID = 0X07;
    public final static int REQ_CODE_MEM = 0X08;

    public final static int RES_CODE_SUCCESS = 0x0;
    public final static int RES_CODE_NO_KEY = 0x1;
    public final static int RES_CODE_NO_MEM = 0x2;
    public final static int RES_CODE_OVERLOAD = 0x3;
    public final static int RES_CODE_INTERNAL_ER = 0x4;
    public final static int RES_CODE_INVALID_OPCODE = 0x5;
    public final static int RES_CODE_INVALID_KEY = 0x6;
    public final static int RES_CODE_INVALID_VALUE = 0x7;
    public final static int RES_CODE_INVALID_OPTIONAL = 0x21;
    public final static int RES_CODE_RETRY_NOT_EQUAL = 0x22;

    public KVClient(InetAddress serverAddress, int serverPort, DatagramSocket socket, byte[] publicBuf) throws SocketException {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.socket = socket;
        this.publicBuf = publicBuf;

        socket.setSoTimeout(100);
    }

    @Override
    public Boolean call() throws Exception {
        return isAlive() && getPID();
    }

    private boolean getPID() throws NoSuchAlgorithmException, IOException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_PID);

        UnwrappedMessage msg = generateMessage(pl);
        byte[] msgb = KVMsgSerializer.serialize(msg);
        DatagramPacket p = new DatagramPacket(msgb, msgb.length, serverAddress, serverPort);

        socket.send(p);

        ServerResponse res;
        try {
            res = receiveServerResponse(msg);
        } catch (ServerTimedOutException e) {
            System.out.println("Server timed out, aborting");
            return false;
        }

        if (res.errcode != RES_CODE_SUCCESS) {
            System.out.println("Server could not return the PID");
            return false;
        }

        if(!res.hasPID) {
            System.out.println("Server did not respond with the PID");
            return false;
        }

        System.out.println("Server PID: " + res.getPid());
        return true;
    }
    private boolean isAlive() throws NoSuchAlgorithmException, IOException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_ALI);

        UnwrappedMessage msg = generateMessage(pl);
        byte[] msgb = KVMsgSerializer.serialize(msg);
        DatagramPacket p = new DatagramPacket(msgb, msgb.length, serverAddress, serverPort);

        socket.send(p);

        ServerResponse res;
        try {
            res = receiveServerResponse(msg);
        } catch (ServerTimedOutException e) {
            System.out.println("Server timed out, aborting");
            return false;
        }

        if (res.errcode != RES_CODE_SUCCESS) {
            System.out.println("Server claims to be not alive, aborting");
            return false;
        }

        System.out.println("Is alive test OK!");
        return true;
    }

     byte[] generateMsgID() throws UnknownHostException {
        byte[] rand = new byte[2];
        try {
            SecureRandom.getInstanceStrong().nextBytes(rand);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        ByteBuffer idAndPl = ByteBuffer.allocate(16);
        idAndPl.order(ByteOrder.LITTLE_ENDIAN);
        idAndPl.put(Inet4Address.getLocalHost().getAddress());
        idAndPl.putShort((short) (socket.getPort() - 32768));
        idAndPl.put(rand);
        idAndPl.putLong(System.nanoTime());

        return idAndPl.array();
    }

    UnwrappedMessage generateMessage(UnwrappedPayload pl) throws UnknownHostException {
        byte[] plb = KVRequestSerializer.serialize(pl);

        ByteBuffer idAndpl = ByteBuffer.allocate(16 + plb.length);

        byte[] msgID = generateMsgID();

        idAndpl.put(msgID);
        idAndpl.put(plb);
        idAndpl.flip();

        CRC32 crc32 = new CRC32();
        crc32.update(idAndpl.array());
        long checksum = crc32.getValue();

        UnwrappedMessage msg = new UnwrappedMessage();
        msg.setCheckSum(checksum);
        msg.setPayload(plb);
        msg.setMessageID(msgID);
        return msg;
    }

    ServerResponse receiveServerResponse(UnwrappedMessage req) throws ServerTimedOutException, IOException {
        DatagramPacket rP = new DatagramPacket(publicBuf, publicBuf.length);

        int tries = 0;
        boolean success = false;

        UnwrappedMessage res = null;
        while (tries < 3 && !success)
        {
            try {
                socket.receive(rP);
            } catch (SocketTimeoutException e) {
                System.out.println("Timed out");
                tries++;
                continue;
                //do nothing and let it loop
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            //verify message ID and CRC
            byte[] trimmed = Arrays.copyOf(rP.getData(), rP.getLength());
            res = (UnwrappedMessage) KVMsgSerializer.parseFrom(new KVMsgFactory(), trimmed);

            boolean msgIDMatch = res.hasMessageID() && Arrays.equals(res.getMessageID(), req.getMessageID());

            ByteBuffer rIDnPL = ByteBuffer.allocate(res.getMessageID().length + res.getPayload().length);
            rIDnPL.put(res.getMessageID());
            rIDnPL.put(res.getPayload());
            rIDnPL.flip();

            CRC32 crc32 = new CRC32();
            crc32.update(rIDnPL.array());
            boolean crc32Match = crc32.getValue() == res.getCheckSum();

            success = msgIDMatch && crc32Match;
            tries++;
        }


        if(tries == 3 && !success) {
            System.out.println("Did not receive response in time, aborting");
            throw new ServerTimedOutException();
        }

        ServerResponse plr = (ServerResponse) KVResponseSerializer.parseFrom(new KVResponseFactory(), res.getPayload());

        return plr;
    }

    class ServerTimedOutException extends Exception{}

}
