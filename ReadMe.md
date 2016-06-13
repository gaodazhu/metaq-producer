#bug fix
1. add crc32 CheckSum
2. fix java consumer error bug
3. fix partition router bug
4. fix sending too fast causes the timeout error by adding atomic operation
5. fix a big bug  topic in demo  used with  a constant
# usage
```
var Producer = require("metaq-producer");
var p = new Producer("meta-test","dn1:2181,dn2:2181,dn3:2181/meta",function(result){
    console.info(result);
    if(result && result.stat == "200"){
        result.flag && result.flag();
        console.info("send success！")
    }else{
        result.flag && result.flag();
        console.info("send error！")
    }
},function(result,err){
    result.flag && result.flag();
    console.info( "send error！"+ err)
}).start(function(){
    var name = "test";
    var i = 1;
    setInterval(function(){
        var msg = name + i;
        var flag = function(){
            console.info("I am flag!");
        };
        p.sendMessage(msg,flag);
        i++;
    },0);
});
```
