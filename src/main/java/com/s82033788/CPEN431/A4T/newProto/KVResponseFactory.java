package com.s82033788.CPEN431.A4T.newProto;

import com.s82033788.CPEN431.A4T.wrappers.ServerResponse;
import com.s82033788.CPEN431.A4T.wrappers.UnwrappedPayload;

public class KVResponseFactory implements MessageFactory{
    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVResponse")) return new ServerResponse();
        throw new IllegalArgumentException("Unknown message name: " + fullMessageName);
    }
}