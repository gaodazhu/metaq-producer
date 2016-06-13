var zookeeper = require("mlsc-zookeeper");
var url = require("url");
var net = require("net");
var util = require("util");
var CRC32 = require("crc32-java");
var ByteBuffer = require('byte');

var ByteUtil = function () {
    var sizeTable = [9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, 2147483647];
    var stringPositiveSize = function (x) {
        for (var i = 0; ; i++) {
            if (x <= sizeTable[i]) {
                return i + 1;
            }
        }
    }
    var stringPositiveSizeLong = function (x) {
        var p = 10;
        for (var i = 1; i < 19; i++) {
            if (x < p) {
                return i;
            }
            p = 10 * p;
        }
        return 19;
    }
    var pub = {
        stringSize: function (x) {
            if (x == 2147483647)
                return 10;
            if (x == -2147483648)
                return 11;
            if (x >= 0) {
                return stringPositiveSize(x);
            } else {
                return 1 + stringPositiveSizeLong(-x);
            }
        }
    }
    return pub;
}
var JavaInt = function () {
    var pri = {
        val: -2147483648,
        min: -2147483648,
        max: 2147483647
    };
    var pub = {
        get: function () {
            return pri.val;
        },
        increase_and_get: function () {
            pri.val += 1
            if (pri.val >= pri.max - 10)
                pri.val = pri.min;
            return pri.val;
        }
    };
    return pub;
};

module.exports = function (topic, zkServerAndRoot, callback, errorCallBack) {
    var pri = {
        topic: topic || "meta-test",
        zkServerAndRoot: zkServerAndRoot || "localhost:2181/meta",
        callback: callback,
        errorCallBack: errorCallBack,
        brokerIds: [],
        needDestroy: false,
        id2Brokers: {},
        id2Partitions:{},
        idPartition2Clients: {},
        ipMap:undefined,
        opaque: new JavaInt(),
        byteUtil: new ByteUtil(),
        zkClient: null,
        nowTime: new Date().getTime(),
        zkInited : false,
        opa2Msg: {},
        checkTTL: function () {
            setInterval(function () {
                pri.nowTime = new Date().getTime();
                for (var opa in pri.opa2Msg) {
                    if (pri.nowTime - pri.opa2Msg[opa].sendTime > 30000) {
                        pri.errorCallBack && pri.errorCallBack(pri.opa2Msg[opa], "timeout error!");
                        delete pri.opa2Msg[opa];
                    }
                }
                pri.nowTime = new Date().getTime();
            }, 20000);
        },
        initZkClient: function (onStartFun) {
            function init(){
                if(pri.needDestroy)
                    return;
                console.info("metaq-producer try to connect to zookeeper");
                pri.zkClient = zookeeper.createClient(zkServerAndRoot);;
                pri.zkClient.on("closed",function(){
                    pri.zkInited = false;
                    setTimeout(init,60000);
                });
                pri.zkClient.once('connected', function () {
                    pri.updateIpMap(pri.updateBrokerIds);
                    var timer = setInterval(function () {
                        console.info("Try to connect to metaq zookeeper！");
                        if (pri.brokerIds.length > 0 && pri.id2Brokers[pri.brokerIds[0]]) {
                            clearInterval(timer);
                            pri.zkInited = true;
                            console.log('Connected to metaq zookeeper！.');
                            onStartFun && onStartFun();
                        }
                    }, 2000);
                });
                pri.zkClient.connect();
            }
            init();
        },
        updateBroker: function (brokerId) {
            brokerId && pri.zkClient && pri.zkClient.isConnected() && pri.zkClient.getData(
                    "/brokers/ids/" + brokerId,
                function (error, data, stat) {
                    if (error) {
                        console.log("Acquire data from zookeeper error!" + error);
                        return;
                    }
                    data = data + "";
                    try {
                        var uri = url.parse(data);
                        if (uri.protocol != "meta:")
                            return;
                        pri.id2Brokers[brokerId] = uri;
                    } catch (e) {
                    }
                }
            );
        },
        updateIpMap:function(callback){
            if(!pri.ipMap){
                callback && callback();
                return;
            }
            pri.zkClient.getData("/meta/IpMap",function (error, data, stat){
                if (error) {
                    console.log("Acquire data from zookeeper error!");
                    return;
                }
                try {
                    pri.ipMap = JSON.parse(data);
                } catch (e) {
                }
                callback && callback();
            });
        },
        updatePartition : function(brokerId){
            brokerId && pri.zkClient && pri.zkClient.isConnected() && pri.zkClient.getData(
                    "/brokers/topics/"+pri.topic+"/" + brokerId +"-m",
                function (error, data, stat) {
                    if (error) {
                        console.log("Acquire data from zookeeper error!" + error);
                        return;
                    }
                    var partitionNum = parseInt(data);
                    pri.id2Partitions[brokerId] = {partitionNum:partitionNum,index : new JavaInt()}
                }
            );
        },
        createClient: function (brokerId,partiton) {
            var client = new net.Socket();
            var uri = pri.id2Brokers[brokerId];
            if (!uri)
                return null;
            var realhost = uri.hostname;
            var realport = uri.port;
            if(pri.ipMap && pri.ipMap[uri.hostname+"_"+uri.port]){
                var urisplit = pri.ipMap[uri.hostname+"_"+uri.port].split("_");
                realhost = urisplit[0];
                realport = parseInt(urisplit[1]);
            }
            client.connect(realport, realhost, function () {
                console.log('Producer has connected to: ' + uri.hostname + ':' + uri.port);
            });
            client.on("data", function (data) {
                var result = pri.formatResult(data);
                delete pri.opa2Msg[result.opa];
                callback && callback(result);
            });
            client.on("error", function (err) {
                client.end();
                delete pri.idPartition2Clients[client.idPartition];
            });
            client.on("close", function () {
                delete pri.idPartition2Clients[client.idPartition];
            });
            client.idPartition = brokerId+"-"+partiton;
            pri.idPartition2Clients[brokerId+"-"+partiton] = client;
            return client;
        },
        updateBrokerIds: function () {
            pri.zkClient && pri.zkClient.isConnected() && pri.zkClient.getChildren(
                    "/brokers/topics/"+pri.topic,
                function (event) {
                    pri.updateBrokerIds();
                },
                function (error, children, stat) {
                    if (error) {
                        console.log("Acquire data from zookeeper error!" + error);
                        return;
                    }
                    pri.brokerIds = [];
                    pri.id2Brokers = {};
                    children.forEach(function (brokerStr) {
                        var brokerId = brokerStr.split("-m")[0];
                        pri.brokerIds.indexOf(brokerId) < 0 && pri.brokerIds.push(brokerId);
                        pri.updatePartition(brokerId);
                        pri.updateBroker(brokerId);
                    });
                }
            );
        },
        formatResult: function (data) {
            try {
                data = data + "";
                var dataSplits = data.split(" ");
                if (dataSplits[0] != "result") {
                    console.log("result head error,except is 'result',but is '" + dataSplits[0] + "'");
                    return null;
                }
                var flagAndOffset = dataSplits[3].split("\r\n");
                var result = {
                    stat: dataSplits[1],
                    opa: flagAndOffset[0],
                    offset: flagAndOffset[1],
                    brokerId: dataSplits[4]
                };
                if (pri.opa2Msg[result.opa]) {
                    result.flag = pri.opa2Msg[result.opa].flag;
                }
                return result;
            } catch (e) {
            }
            return null;
        }
    };


    var pub = {
        start: function (onStartFun) {
            pri.initZkClient(onStartFun);
            pri.checkTTL();
            return this;
        },
        isConnected:function(){
            return pri.zkInited;
        },
        sendMessage: function (msg, myflag) {
            if (pri.needDestroy)
                throw new Error("producer destroyed!");
            if (!myflag)
                console.warn("if do not set flag! will ack msg self!");
            if(!pri.zkInited){
                while(!pri.zkInited){
                    //block it
                }
            }
            try {
                var opa = pri.opaque.increase_and_get();
                var flag = 0;
                var nextIndex = Math.abs(opa % pri.brokerIds.length);
                var brokerId = pri.brokerIds[nextIndex];
                var partition = 0;
                if(pri.id2Partitions[brokerId]){
                    partition = Math.abs(pri.id2Partitions[brokerId].index.increase_and_get() % pri.id2Partitions[brokerId].partitionNum);
                }
                var idPartition = brokerId +"-"+partition;
                if (!pri.idPartition2Clients[idPartition]) {
                    pri.createClient(brokerId,partition);
                    pub.sendMessage(msg, myflag);
                    return;
                }
                var msgStr = typeof(msg) == "string" ? msg : JSON.stringify(msg);
                var len = msgStr.length;
                var crc32 = new CRC32();
                crc32.update1(new Buffer(msgStr), 0, msgStr.length)
                var checksum = crc32.getValue() & 0x7FFFFFFF;
                var byteSize = 11 + pri.byteUtil.stringSize(brokerId) + pri.byteUtil.stringSize(len)
                    + pri.byteUtil.stringSize(opa) + pri.topic.length
                    + pri.byteUtil.stringSize(0)
                    + pri.byteUtil.stringSize(checksum) + len;
                var bb = ByteBuffer.allocate(byteSize);
                bb.putRawString("put").putRawString(" ").putRawString(pri.topic)
                    .putRawString(" ").putRawString(partition+"").putRawString(" ")
                    .putRawString(len+"").putRawString(" ").putRawString(flag+"")
                    .putRawString(" ").putRawString(checksum+"").putRawString(" ")
                    .putRawString(opa+"").putRawString("\r\n").putRawString(msgStr);
                bb.flip();
                pri.idPartition2Clients[idPartition].write(bb._bytes);
                pri.opa2Msg[opa] = {sendTime: pri.nowTime, flag: myflag || msg};
            } catch (e) {
                pri.errorCallBack && pri.errorCallBack({flag: myflag || msg}, e);
            }
        },
        destroy: function () {
            pri.zkClient.close();
            pri.needDestroy = true;
            for (var idPartition in pri.idPartition2Clients) {
                pri.idPartition2Clients[idPartition].end();
            }
        }
    }
    return pub;
}

