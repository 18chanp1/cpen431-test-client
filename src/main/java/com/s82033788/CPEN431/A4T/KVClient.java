package com.s82033788.CPEN431.A4T;

import com.s82033788.CPEN431.A4T.newProto.*;
import com.s82033788.CPEN431.A4T.wrappers.ServerResponse;
import com.s82033788.CPEN431.A4T.wrappers.UnwrappedMessage;
import com.s82033788.CPEN431.A4T.wrappers.UnwrappedPayload;

import java.io.IOException;
import java.math.BigInteger;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.zip.CRC32;

public class KVClient implements Callable<Integer> {
    private InetAddress serverAddress;
    private int serverPort;
    private DatagramSocket socket;
    byte[] publicBuf;
    int testSequence;
    UnwrappedMessage messageOnWire;

    /* Test Result codes */
    public final static int TEST_FAILED = 0;
    public final static int TEST_PASSED = 1;
    public final static int TEST_UNDECIDED = 2;

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

    public KVClient(InetAddress serverAddress, int serverPort, DatagramSocket socket, byte[] publicBuf, int testSequence) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.socket = socket;
        this.publicBuf = publicBuf;
        this.testSequence = testSequence;
    }

    @Override
    public Integer call() throws Exception {
        return threadSafeTest();
    }

    public int threadSafeTest() {
        System.out.println("Starting Thread Safe Test. PUT -> GET");
        ByteBuffer tidb = ByteBuffer.allocate(Long.BYTES);
        tidb.putLong(Thread.currentThread().threadId());
        tidb.flip();

        byte[] key = tidb.array();
        byte[] value = new byte[64];
        int version = 0;

        long end = System.currentTimeMillis() + 60000 ;
        long i = 0;

        while (System.currentTimeMillis() < end)
        {
            try {
                SecureRandom.getInstanceStrong().nextBytes(value);
                version = SecureRandom.getInstanceStrong().nextInt();
            } catch (NoSuchAlgorithmException e) {
                System.out.println("Unable to generate random items. Aborting");
                return TEST_UNDECIDED;
            }

            ServerResponse p;
            try {
                p = put(key, value, version);
            } catch (Exception e) {
                return handleException(e);
            }

            ServerResponse g1;
            try {
                g1 = get(key);
            } catch (Exception e) {
                return handleException(e);
            }

            if(g1.getErrCode() != RES_CODE_SUCCESS){
                System.out.println("GET failed with error code: " + p.getErrCode());
                return TEST_FAILED;
            }

            if (!Arrays.equals(g1.getValue(), value)) {
                System.out.println("GET value did not match. Put: " + value + " Get: " + g1.getValue());
                return TEST_FAILED;
            }
            i++;
        }


        System.out.println("Closed loops: " + i);
        System.out.println("*** TEST PASSED ***");
        return TEST_PASSED;
    }

    /* Different sequences of tests*/
    public int basicOperationsTest() throws IOException {
        System.out.println("Basic Operations Test. PUT -> GET -> REMOVE -> GET -> REMOVE -> PUT -> GET");

        /* Generate some random values for key, value, and version */
        byte[] key =  new byte[32];
        byte[] value = new byte[64];
        int version;

        try {
            SecureRandom.getInstanceStrong().nextBytes(key);
            SecureRandom.getInstanceStrong().nextBytes(value);
            version = SecureRandom.getInstanceStrong().nextInt();
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }

        /* PUT operation */
        ServerResponse p;
        System.out.println("Putting value 1");
        try {
            p = put(key, value, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PUT failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        /* GET Operation. */
        System.out.println("Getting value 1");
        ServerResponse g1;
        try {
            g1 = get(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(g1.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("GET failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        if (!Arrays.equals(g1.getValue(), value)) {
            System.out.println("GET value did not match. Put: " + value + " Get: " + g1.getValue());
            return TEST_FAILED;
        }

        if (g1.getVersion() != version) {
            System.out.println("GET version did not match. Put: " + value + " Get: " + g1.getValue());
            return TEST_FAILED;
        }

        /* delete Operation*/
        System.out.println("Deleting value 1");
        ServerResponse d1;
        try {
            d1 = delete(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(d1.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("DEL failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        /* GET Operation */
        System.out.println("Getting non existent value");
        ServerResponse g2;
        try {
            g2 = get(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(g2.getErrCode() != RES_CODE_NO_KEY){
            System.out.println("GET did not return no key. Error Code:" + p.getErrCode());
            return TEST_FAILED;
        }

        /* DEL operation*/
        ServerResponse d2;
        System.out.println("Deleting non existent value");
        try {
            d2 = delete(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(d2.getErrCode() != RES_CODE_NO_KEY){
            System.out.println("DEL did not return no key. Error Code:" + p.getErrCode());
            return TEST_FAILED;
        }

        /* PUT operation, with new values */
        ServerResponse p2;
        System.out.println("Putting value 2");
        try {
            SecureRandom.getInstanceStrong().nextBytes(value);
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }
        version += 20;

        try {
            p2 = put(key, value, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p2.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PUT failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        /* GET Operation */
        ServerResponse g3;
        System.out.println("Get value 2");
        try {
            g3 = get(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(g3.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("GET failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        if (!Arrays.equals(g3.getValue(), value)) {
            System.out.println("GET value did not match. Put: " + value + " Get: " + g3.getValue());
            return TEST_FAILED;
        }

        if (g3.getVersion() != version) {
            System.out.println("GET version did not match. Put: " + value + " Get: " + g3.getValue());
            return TEST_FAILED;
        }


        System.out.println("Basic Operations Test complete");
        System.out.println("***PASSED***");
        return TEST_PASSED;
    }

    public int garbageTest() throws IOException {
        System.out.println("Garbage resistance test");
        byte[] garbage = new byte[13000];

        try {
            SecureRandom.getInstanceStrong().nextBytes(garbage);
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }

        DatagramPacket d = new DatagramPacket(garbage, garbage.length, serverAddress, serverPort);
        socket.send(d);

        System.out.println("Checking server is alive");
        ServerResponse a;
        try {
            a = isAlive();
        } catch (Exception e) {
            return handleException(e);
        }

        if(a.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("Server is allegedly dead." + a.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("*** PASSED ***");
        return TEST_PASSED;
    }


    public int atMostOnceTest() throws IOException {
        System.out.println("At most once Test. PUT -> DEL -> DEL");

        /* Generate some random values for key, value, and version */
        byte[] key =  new byte[32];
        byte[] value = new byte[64];
        int version;

        try {
            SecureRandom.getInstanceStrong().nextBytes(key);
            SecureRandom.getInstanceStrong().nextBytes(value);
            version = SecureRandom.getInstanceStrong().nextInt();
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }

        /* PUT operation */
        ServerResponse p;
        System.out.println("Putting value 1");
        try {
            p = put(key, value, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PUT failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }


        /* delete Operation*/
        System.out.println("Deleting value 1");
        ServerResponse d1;
        try {
            d1 = delete(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(d1.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("DEL failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        /* Delete Attempt 2*/
        System.out.println("Deleting value 1 again");
        ServerResponse d2;
        try {
            d2 = sendAndReceiveSingleServerResponse(messageOnWire);
        } catch (Exception e) {
            return handleException(e);
        }

        if (d2.getErrCode() == RES_CODE_NO_KEY) {
            System.out.println("DEL At most once failed. Returned no key instead of previous response");
            return TEST_FAILED;
        }
        else if(d2.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("DEL failed with error code: " + d2.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("*** Part 1 Passed ***");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            System.out.println("Sleep interrupted");
            return TEST_UNDECIDED;
        }

        System.out.println("At most once Test2.  REM -> PUT -> REM (Same as first one)");

        System.out.println("Performing WIPEOUT");
        ServerResponse w;
        try {
            w = wipeout();
        } catch (Exception e) {
            return handleException(e);
        }

        if(w.getErrCode() != RES_CODE_SUCCESS) {
            System.out.println("Unable to wipeout");
            return TEST_FAILED;
        }


        /* delete Operation*/
        System.out.println("Deleting value 1");
        ServerResponse d3;
        try {
            d3 = delete(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(d3.getErrCode() != RES_CODE_NO_KEY){
            System.out.println("DEL expected no key error: " + d3.getErrCode());
            return TEST_FAILED;
        }

        UnwrappedMessage d3S = messageOnWire;

        ServerResponse p2;
        System.out.println("putting value 1");
        try {
            p2 = put(key, value, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p2.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PUT failed with error code: " + p2.getErrCode());
            return TEST_FAILED;
        }


        System.out.println("Resend Deleting value 1");
        ServerResponse d4;
        try {
            d4 = sendAndReceiveSingleServerResponse(d3S);
        } catch (Exception e) {
            return handleException(e);
        }

        if(d4.getErrCode() != RES_CODE_NO_KEY){
            System.out.println("DEL expected no key error: " + d4.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("*** PART 2 PASS ***");

        System.out.println("At most once test 3 PUT -> REM -> Many PUT -> REM");

        try {
            SecureRandom.getInstanceStrong().nextBytes(key);
            SecureRandom.getInstanceStrong().nextBytes(value);
            version = SecureRandom.getInstanceStrong().nextInt();
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }

        ServerResponse p4;
        System.out.println("Putting value 1");
        try {
            p4 = put(key, value, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p4.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PUT failed with error code: " + p4.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("Deleting value 1");
        ServerResponse d5;
        try {
            d5 = delete(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(d5.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("DEL failed with error code: " + d5.getErrCode());
            return TEST_FAILED;
        }

        UnwrappedMessage d5s = messageOnWire;

        for(int i = 0; i < 30; i++) {
            try {
                SecureRandom.getInstanceStrong().nextBytes(key);
                SecureRandom.getInstanceStrong().nextBytes(value);
                version = SecureRandom.getInstanceStrong().nextInt();
            } catch (NoSuchAlgorithmException e) {
                System.out.println("Unable to generate random items. Aborting");
                return TEST_UNDECIDED;
            }

            ServerResponse p5;
            System.out.println("Putting value (random)");
            try {
                p5 = put(key, value, version);
            } catch (Exception e) {
                return handleException(e);
            }

            if(p5.getErrCode() != RES_CODE_SUCCESS){
                System.out.println("PUT failed with error code: " + p5.getErrCode());
                return TEST_FAILED;
            }

        }

        System.out.println("Deleting value 1 (retry)");
        ServerResponse d6;
        try {
            d6 = sendAndReceiveSingleServerResponse(d5s);
        } catch (Exception e) {
            return handleException(e);
        }

        if(d6.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("DEL failed with error code: " + d6.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("*** PART 3 PASS ***");

        System.out.println("At most once test 4 PUT1 -> PUT2 -> PUT1 -> GET2");

        try {
            SecureRandom.getInstanceStrong().nextBytes(key);
            SecureRandom.getInstanceStrong().nextBytes(value);
            version = SecureRandom.getInstanceStrong().nextInt();
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }

        ServerResponse p6;
        System.out.println("Putting value 1");
        try {
            p6 = put(key, value, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p6.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PUT failed with error code: " + p6.getErrCode());
            return TEST_FAILED;
        }

        UnwrappedMessage p6s = messageOnWire;


        try {
            SecureRandom.getInstanceStrong().nextBytes(value);
            version = SecureRandom.getInstanceStrong().nextInt();
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }


        ServerResponse p7;
        System.out.println("Putting value 2");
        try {
            p7 = put(key, value, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p7.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PUT failed with error code: " + p7.getErrCode());
            return TEST_FAILED;
        }

        byte[] val1 = Arrays.copyOf(value, value.length);
        int v1 = version;

        ServerResponse p8;
        System.out.println("Resend put value 1");
        try {
            p8 = sendAndReceiveSingleServerResponse(p6s);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p8.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PUT failed with error code: " + p8.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("Getting value 2");
        ServerResponse g1;
        try {
            g1 = get(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(g1.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("GET did not return no key. Error Code:" + g1.getErrCode());
            return TEST_FAILED;
        }

        if(!Arrays.equals(val1, g1.getValue()) || v1 != g1.getVersion()) {
            System.out.println("Expected old value: " + val1 + ", Actual: " + g1.getValue());
            return TEST_FAILED;
        }

        System.out.println("*** PART 4 PASS ***");
        return TEST_PASSED;
    }

    public int invalidCommandTest() throws IOException {
        System.out.println("Testing invalid command");
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(0x69);

        ServerResponse i;

        try {
            i = sendAndReceiveServerResponse(pl);
        } catch (Exception e) {
            return handleException(e);
        }

        if (i.getErrCode() != RES_CODE_INVALID_OPCODE) {
            System.out.println("Invalid cmd did not return invalid. Error Code:" + i.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("*** TEST PASSED ***");
        return TEST_PASSED;
    }

    public int memoryTest() throws IOException {
        System.out.println("Memory Violation Test PUT. PUT->GET Closed Loop, 10kbytes");

        System.out.println("Performing WIPEOUT");
        ServerResponse w;
        try {
            w = wipeout();
        } catch (Exception e) {
            return handleException(e);
        }

        if(w.getErrCode() != RES_CODE_SUCCESS) {
            System.out.println("Unable to wipeout");
            return TEST_FAILED;
        }



        BigInteger i = BigInteger.ZERO;
        byte[] value = new byte[10000];

        try {
            SecureRandom.getInstanceStrong().nextBytes(value);
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }

        ServerResponse p;
        try {
            p = put(i.toByteArray(), value,0);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p.getErrCode() != RES_CODE_SUCCESS) {
            System.out.println("Unable to put first item. Aborting");
            return TEST_FAILED;
        }

        ServerResponse g;
        do {
            try {
                g = get(i.toByteArray());
            } catch (Exception e) {
                return handleException(e);
            }

            if(!Arrays.equals(g.getValue(), value)){
                System.out.println("GET value did not match. Put: " + value + " Get: " + g.getValue());
                return TEST_FAILED;
            }

            i = i.add(BigInteger.ONE);
            try {
                SecureRandom.getInstanceStrong().nextBytes(value);
            } catch (NoSuchAlgorithmException e) {
                System.out.println("Unable to generate random items. Aborting");
                return TEST_UNDECIDED;
            }

            try {
                p = put(i.toByteArray(), value,0);
            } catch (Exception e) {
                return handleException(e);
            }
        } while (p.getErrCode() == RES_CODE_SUCCESS || i.longValue()> 6710);

        if(p.getErrCode() != RES_CODE_NO_MEM) {
            System.out.println("Error occurred. Error Code: " + p.getErrCode() );
            return TEST_FAILED;
        }

        if(i.longValue() > 6710){
            System.out.println("Memory capacity exceeded.");
            return TEST_FAILED;
        }

        System.out.println("Closed loops:" + i.longValue());
        System.out.println("System Memory: " + (i.longValue() * 10000) / 1048576F + "MiB");
        System.out.println("***PASSED***");

        return TEST_PASSED;
    }
    public int illegalKVTest() throws IOException {
        System.out.println("Illegal KV Test.");
        System.out.println("PUT (illegal key) -> PUT (illegal value)");
        System.out.println("-> GET (Illegal Key) -> DELETE (Illegal Key)");

        byte[] key = new byte[16];
        byte[] illegalKey = new byte[64];
        byte[] value = new byte[64];
        byte[] illegalValue = new byte[10001];
        int version;

        try {
            SecureRandom.getInstanceStrong().nextBytes(key);
            SecureRandom.getInstanceStrong().nextBytes(illegalKey);
            SecureRandom.getInstanceStrong().nextBytes(value);
            SecureRandom.getInstanceStrong().nextBytes(illegalValue);
            version = SecureRandom.getInstanceStrong().nextInt();
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }

        ServerResponse p1;
        System.out.println("Putting Illegal Key");
        try {
            p1 = put(illegalKey, value, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p1.getErrCode() != RES_CODE_INVALID_KEY){
            System.out.println("PUT did not return key too long. Error code: " + p1.getErrCode());
            return TEST_FAILED;
        }

        ServerResponse p2;
        System.out.println("Putting illegal value");
        try {
            p2 = put(key, illegalValue, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p2.getErrCode() != RES_CODE_INVALID_VALUE){
            System.out.println("PUT did not return value too long. Error code: " + p2.getErrCode());
            return TEST_FAILED;
        }

        ServerResponse g;
        System.out.println("Getting illegal key");
        try {
            g = get(illegalKey);
        } catch (Exception e) {
            return handleException(e);
        }

        if(g.getErrCode() != RES_CODE_INVALID_KEY){
            System.out.println("GET did not return key too long. Error code: " + p2.getErrCode());
            return TEST_FAILED;
        }

        ServerResponse d;
        System.out.println("Deleting illegal key");
        try {
            d = get(illegalKey);
        } catch (Exception e) {
            return handleException(e);
        }

        if(d.getErrCode() != RES_CODE_INVALID_KEY){
            System.out.println("PUT did not return key too long. Error code: " + p2.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("***PASSED***");
        return TEST_PASSED;
    }
    public int basicWipeoutTest() throws IOException {
        System.out.println("Basic Wipeout Test. PUT -> WIP -> GET -> REMOVE");

        /* Generate some random values for key, value, and version */
        byte[] key =  new byte[32];
        byte[] value = new byte[64];
        int version;

        try {
            SecureRandom.getInstanceStrong().nextBytes(key);
            SecureRandom.getInstanceStrong().nextBytes(value);
            version = SecureRandom.getInstanceStrong().nextInt();
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }

        ServerResponse p;
        System.out.println("Putting value 1");
        try {
            p = put(key, value, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PUT failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        /* delete Operation*/
        System.out.println("Wiping out server");
        ServerResponse w;
        try {
            w = wipeout();
        } catch (Exception e) {
            return handleException(e);
        }

        if(w.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("WIPEOUT failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        /* GET Operation */
        System.out.println("Getting non existent value");
        ServerResponse g1;
        try {
            g1 = get(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(g1.getErrCode() != RES_CODE_NO_KEY){
            System.out.println("GET did not return no key. Error Code:" + p.getErrCode());
            return TEST_FAILED;
        }

        /* DEL operation*/
        ServerResponse d;
        System.out.println("Deleting non existent value");
        try {
            d = delete(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(d.getErrCode() != RES_CODE_NO_KEY){
            System.out.println("DEL did not return no key. Error Code:" + p.getErrCode());
            return TEST_FAILED;
        }

        /* PUT operation, with new values */
        ServerResponse p2;
        System.out.println("Putting value 2");
        try {
            SecureRandom.getInstanceStrong().nextBytes(value);
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Unable to generate random items. Aborting");
            return TEST_UNDECIDED;
        }
        version += 20;

        try {
            p2 = put(key, value, version);
        } catch (Exception e) {
            return handleException(e);
        }

        if(p2.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PUT failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        /* GET Operation */
        ServerResponse g2;
        System.out.println("Get value 2");
        try {
            g2 = get(key);
        } catch (Exception e) {
            return handleException(e);
        }

        if(g2.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("GET failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        if (!Arrays.equals(g2.getValue(), value)) {
            System.out.println("GET value did not match. Put: " + value + " Get: " + g2.getValue());
            return TEST_FAILED;
        }

        if (g2.getVersion() != version) {
            System.out.println("GET version did not match. Put: " + value + " Get: " + g2.getValue());
            return TEST_FAILED;
        }


        System.out.println("Basic Wipeout Test complete");
        System.out.println("***PASSED***");
        return TEST_PASSED;
    }
    public int administrationTest() throws IOException {
        System.out.println("Administration Test. PID -> ALI -> MEM -> SHU -> ALI");

        /* Generate some random values for key, value, and version */

        ServerResponse p;
        System.out.println("Getting PID");
        try {
            p = getPID();
        } catch (Exception e) {
            return handleException(e);
        }

        if(p.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("PID failed with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("Checking server is alive");
        ServerResponse a;
        try {
            a = isAlive();
        } catch (Exception e) {
            return handleException(e);
        }

        if(a.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("ALI with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        System.out.println("Checking Membership Count");
        ServerResponse m;
        try {
            m = getMembershipCount();
        } catch (Exception e) {
            return handleException(e);
        }

        if(m.getErrCode() != RES_CODE_SUCCESS){
            System.out.println("MEM with error code: " + p.getErrCode());
            return TEST_FAILED;
        }

        if(m.getMembershipCount() != 1) {
            System.out.println("MEM count mismatch. Count:" + p.getMembershipCount());
            return TEST_FAILED;
        }

        try {
            shutdown();
        } catch (ServerTimedOutException e) {
            System.out.println("Server shut down");
        } catch (Exception e) {
            return handleException(e);
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            System.out.println("Sleep interrupted");
            return TEST_UNDECIDED;
        }

        //Test isAlive
        try {
            isAlive();
        } catch (ServerTimedOutException e) {
            System.out.println("Server is not alive after shutdown");
            System.out.println("***PASSED***");
            return TEST_PASSED;
        } catch (Exception e) {
            return handleException(e);
        }

        System.out.println("Server is still alive");
        return TEST_FAILED;
    }


   /* Helper functions corresponding to each type of request */
    private void shutdown() throws IOException, InterruptedException, ServerTimedOutException, MissingValuesException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_SHU);

        System.out.println("Sleeping for 5s");
        Thread.sleep(5000);


        sendAndReceiveServerResponse(pl);
    }
    private ServerResponse wipeout() throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_WIP);


        return sendAndReceiveServerResponse(pl);
    }
    private ServerResponse delete(byte[] key) throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_DEL);
        pl.setKey(key);


        return sendAndReceiveServerResponse(pl);
    }
    private ServerResponse get(byte[] key) throws IOException, MissingValuesException, ServerTimedOutException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_GET);
        pl.setKey(key);


        ServerResponse res = sendAndReceiveServerResponse(pl);

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasValue())
            throw new MissingValuesException("Value");

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasVersion())
            throw new MissingValuesException("Version");

        return res;
    }
    private ServerResponse put(byte[] key, byte[] value, int version) throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_PUT);
        pl.setKey(key);
        pl.setValue(value);
        pl.setVersion(version);

        return sendAndReceiveServerResponse(pl);
    }
    private ServerResponse getMembershipCount() throws IOException, MissingValuesException, ServerTimedOutException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_MEM);

        ServerResponse res = sendAndReceiveServerResponse(pl);

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasMembershipCount())
            throw new MissingValuesException("Membership Count");

        return res;
    }
    private ServerResponse getPID() throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_PID);

        ServerResponse res = sendAndReceiveServerResponse(pl);

        if(res.getErrCode() == RES_CODE_SUCCESS && !res.hasPid())
            throw new MissingValuesException("PID");

        return res;
    }
    private ServerResponse isAlive() throws IOException, ServerTimedOutException, MissingValuesException, InterruptedException {
        /* Generate isAlive Message */
        UnwrappedPayload pl = new UnwrappedPayload();
        pl.setCommand(REQ_CODE_ALI);

        return sendAndReceiveServerResponse(pl);
    }

    /* Utilities to generate and process outgoing and incoming packets following the retry and At most once policy */
    private ServerResponse sendAndReceiveServerResponse(UnwrappedPayload pl) throws
            IOException,
            ServerTimedOutException,
            InterruptedException,
            MissingValuesException {
        ServerResponse res;

        UnwrappedMessage msg = generateMessage(pl);
        res = sendAndReceiveSingleServerResponse(msg);

       /* Send a new packet after overload time*/
        while(res.getErrCode() == RES_CODE_OVERLOAD){
            if(!res.hasOverloadWaitTime()) throw new MissingValuesException("Overload wait time");
            Thread.sleep(res.getOverloadWaitTime());

            msg = generateMessage(pl);
            res = sendAndReceiveSingleServerResponse(msg);
        }

        return res;
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

    ServerResponse sendAndReceiveSingleServerResponse(UnwrappedMessage req) throws ServerTimedOutException, IOException {
        DatagramPacket rP = new DatagramPacket(publicBuf, publicBuf.length);

        int tries = 0;
        boolean success = false;

        byte[] msgb = KVMsgSerializer.serialize(req);
        DatagramPacket p = new DatagramPacket(msgb, msgb.length, serverAddress, serverPort);
        int initTimeout = socket.getSoTimeout();

        messageOnWire = req;

        UnwrappedMessage res = null;
        while (tries < 3 && !success)
        {
            socket.send(p);
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
            socket.setSoTimeout(socket.getSoTimeout() * 2);
        }

        socket.setSoTimeout(initTimeout);

        if(tries == 3 && !success) {
            System.out.println("Did not receive response after 3 tries");
            throw new ServerTimedOutException();
        }

        ServerResponse plr = (ServerResponse) KVResponseSerializer.parseFrom(new KVResponseFactory(), res.getPayload());

        return plr;
    }

    /* Exception Handler */
    int handleException(Exception e) {
        if (e instanceof ServerTimedOutException) {
            System.out.println("Server timed out. Aborting");
            return TEST_UNDECIDED;
        } else if (e instanceof  MissingValuesException) {
            System.out.println("Server response did not contain value: " + ((MissingValuesException)e).missingItem);
            return TEST_FAILED;
        } else if (e instanceof InterruptedException) {
            System.out.println("Thread interrupted.");
            return TEST_UNDECIDED;
        } else {
            e.printStackTrace();
            return TEST_UNDECIDED;
        }

    }

    /* Custom exceptions*/
    class ServerTimedOutException extends Exception{}
    class MissingValuesException extends Exception{
        public String missingItem;

        public MissingValuesException(String missingItem) {
            this.missingItem = missingItem;
        }
    }

}
