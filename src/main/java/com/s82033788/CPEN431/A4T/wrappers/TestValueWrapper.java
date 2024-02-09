package com.s82033788.CPEN431.A4T.wrappers;

public class TestValueWrapper {
    private byte[] value;
    int version;
    boolean success;

    public TestValueWrapper(byte[] value, int version ) {
        this.value = value;
        this.version = version;
        this.success = true;
    }

    public TestValueWrapper() {
        this.success = false;
    }

    public boolean isSuccess() {
        return success;
    }

    public byte[] getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }
}
