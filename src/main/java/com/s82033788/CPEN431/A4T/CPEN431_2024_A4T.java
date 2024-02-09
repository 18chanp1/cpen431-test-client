package com.s82033788.CPEN431.A4T;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class CPEN431_2024_A4T {
    public final static String address = "127.0.0.1";
    public final static int port = 13788;
    public static void main (String[] args)
    {
        try {
            PrintStream out = new PrintStream(new FileOutputStream("TestLog.txt"));
            System.setOut(out);

            System.out.println("Starting Paco's Test Client");

            KVClient cl = new KVClient(
                    InetAddress.getByName(address),
                    port,
                    new DatagramSocket(),
                    new byte[16384]
            );

            cl.call();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
