/**
* Created by gaozhu on 2015/3/23.
*/
var Producer = require("./producer.js");
var p = new Producer("monitor","sdn1:2181,sdn2:2181,sdn3:2181/meta",function(result){
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
    var data = [{'time':'2015-03-31 15:55:58.832','points':[{'name':'U05_02_01001','type':1,'value':0.2497},{'name':'U05_02_01003','type':1,'value':0.5316},{'name':'U05_02_01004','type':1,'value':0.5837},{'name':'U05_02_01002','type':1,'value':0.0511},{'name':'U05_02_02001','type':1,'value':0.2695},{'name':'U05_02_02002','type':1,'value':0.8016},{'name':'U05_02_02003','type':1,'value':0.8343},{'name':'U05_02_02004','type':1,'value':0.4221},{'name':'U05_02_02014','type':1,'value':0.8084},{'name':'U05_02_02015','type':1,'value':0.5612}],'ucode':'U05'}];
    var i = 1;
        var time = new Date().getTime();
    var timer = setInterval(function(){
        if(i > 100){
            clearInterval(timer);
            console.info("###########  耗时 ：" + (new Date().getTime() - time));
            return;
        }
        var flag = function(){
            console.info("I am flag!");
        };
        p.sendMessage(data,flag);
        i++;
    },2000);

});
