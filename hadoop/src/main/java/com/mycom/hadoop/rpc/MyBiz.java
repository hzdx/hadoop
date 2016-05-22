package com.mycom.hadoop.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyBiz extends VersionedProtocol {
    long versionID = 12321443L;
    String hello(String name);
}
