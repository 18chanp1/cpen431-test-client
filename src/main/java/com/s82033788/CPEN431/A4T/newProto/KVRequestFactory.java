package com.s82033788.CPEN431.A4T.newProto;

import com.s82033788.CPEN431.A4T.wrappers.UnwrappedPayload;

public class KVRequestFactory implements MessageFactory{
    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVRequest")) return new UnwrappedPayload();
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}
