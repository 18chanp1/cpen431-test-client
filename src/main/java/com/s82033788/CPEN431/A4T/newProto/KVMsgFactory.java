package com.s82033788.CPEN431.A4T.newProto;

import com.s82033788.CPEN431.A4T.wrappers.UnwrappedMessage;

public class KVMsgFactory implements MessageFactory{

    @Override
    public Object create(String fullMessageName) {
        if(fullMessageName.equals("KVMsg")) return new UnwrappedMessage();
        throw new IllegalArgumentException("Unknown Message Name: " + fullMessageName);
    }
}
