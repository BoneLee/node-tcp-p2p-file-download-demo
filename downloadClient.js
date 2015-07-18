/**
 * Created by bone on 15-7-15.
 */

var net = require('net'),
    path = require('path'),
    Nigel = require('nigel'),
    randomAccessFile = require('random-access-file');


var EOF = new Buffer('\r\nEOF\r\n');
var config = {
    BLOCK_SIZE: 64 * 1024, //64KB
    //BLOCK_SIZE: 1024 * 1024, //64KB
    MAX_HTTP_CONNECTION_CNT: 64,
};
var blockID = 0;

var fileBlocksDownloadLeft = {};
var fileBlocksDownloading = {};

var downloadCandidateQueue2 = {};
var downloadActiveQueue2 = {};

function getBlockSize(blockID, fileSize){
    var start =  blockID*config.BLOCK_SIZE;
    var end = (blockID+1)*config.BLOCK_SIZE-1;
    if (end >= fileSize){
        end = fileSize - 1;
    }
    return (end-start+1);
}

function constructHeader(blockID, fileHash, fileSize){
    /*
    if ((typeof blockID) == 'undefined') {
        return null;
    }
    */
    blockID = parseInt(blockID);
    var start =  blockID*config.BLOCK_SIZE;
    var end = (blockID+1)*config.BLOCK_SIZE-1;
    if (end >= fileSize){
        end = fileSize - 1;
    }
    /*
    if(start > end){
        return null;
    }else{
        return new Buffer(JSON.stringify({"start": start, "end": end, "hash": fileHash}));
    }
    */
    return new Buffer(JSON.stringify({"start": start, "end": end, "hash": fileHash}));
}

function fileBlocks(fileSize) {
    return Math.floor((fileSize + config.BLOCK_SIZE - 1) / config.BLOCK_SIZE);
}

function downloadFile(){
    // initialize
    var fileID = '123_134';
    var fileSize = 5537216;
    downloadCandidateQueue2[fileID]=[{'host': '127.0.0.1', 'port': 8885},
        {'host': '::1', 'port': 8885},
        {'host': '192.168.0.108', 'port': 8885}];
    downloadActiveQueue2[fileID] = [];
    fileBlocksDownloadLeft[fileID] = range(0, fileBlocks(fileSize));
    fileBlocksDownloading[fileID] = [];

    var concurrentHttpCnt = downloadCandidateQueue2[fileID].length;
    for (var i = 0; i < 3; ++i) {
        downloadFileHelper(fileID);
    }
}

function downloadFileHelper(fileID) {
     process.nextTick(function () {
         var filePath = path.join(__dirname, 'fill2.rar');
         var owner = pop(downloadCandidateQueue2[fileID]);
         if (owner) {
             downloadActiveQueue2[fileID].push(owner);
             downloadBlockHelper(owner, filePath, fileID); //fileDownloadInfos[fileID], filesToSave[fileID], owner, fileDownloadEventEmitter[fileID], fileDownloadOverCallbacks[fileID]);
         }
     });
}

function downloadBlockHelper(owner, filePath, fileID){
    var HOST = owner['host'];
    var PORT = owner['port'];

    // mock data
    var fileHash = 0; // TODO FIXME
    var fileSize = 5537216; // TODO FIXME
    if(fileBlocksDownloadLeft[fileID].length == 0){
        return;
    }

    var client = new net.Socket();

    /*
    function getNextBlockID(){
        var blockID = pop(fileBlocksDownloadLeft[fileID]);
        if ((typeof blockID) == 'undefined') {
            removeItemOfArray(downloadActiveQueue2[fileID], owner);
            downloadCandidateQueue2[fileID].push(owner);
            client.destroy();
            return null;
        }
        blockID = parseInt(blockID);
        return blockID;
    }
    */
    var blockID = pop(fileBlocksDownloadLeft[fileID]);
    fileBlocksDownloading[fileID].push(blockID);

    client.connect(PORT, HOST, function () {
        console.log('CONNECTED TO: ' + HOST + ':' + PORT);
        // Write a message to the socket as soon as the client is connected, the server will receive it as message from the client
        client.write(constructHeader(blockID, fileHash, fileSize));
    });

    // Add a 'data' event handler for the client socket
    // data is what the server sent to this socket
    var chunks = [];
    var file = randomAccessFile(filePath);

    client.on('data', function (data) {
        //console.log("data length: "+data.length);
        //var eofIndex = data.indexOf(EOF);
        var eofIndex = Nigel.horspool(data, EOF);
        if (eofIndex >= 0) {
            //if(data.toString() == '\r\nEOF\r\n'){
            var pumpData = data.slice(0, eofIndex);
            if (pumpData.length > 0){
                console.log("pump data len:"+pumpData.length);
                chunks.push(pumpData);
            }
            var content = Buffer.concat(chunks);
            chunks = [];
            var leftData = data.slice(eofIndex + EOF.length);
            if (leftData.length > 0) {
                console.log("left data len:"+leftData.length);
                chunks.push(leftData);
            }
            console.log("complete block:" + blockID + " content len:" + content.length);

            // check if content size error.
            var blockSize = getBlockSize(blockID, fileSize);
            if(content.length != blockSize){
                console.log("block size error:"+blockSize+" I will retry again.");
                client.write(constructHeader(blockID, fileHash, fileSize));
                return;
            }

            file.write(blockID * config.BLOCK_SIZE, content,
                function (err) {
                    if (err) {
                        // TODO FIXME
                        console.log("write block to file error, I will destroy socket.");
                        file.close();
                        setTimeout(function(){ client.destroy(); }, 1000);
                    } else {
                        // TODO FIXME
                        //recordGoodOwners(owner, fileID);
                        //recordDownloadedOwners(owner, fileID);
                        removeItemOfArray(fileBlocksDownloading[fileID], blockID);
                        // TODO FIXME updateBlocksToDB
                        console.log("write content block Ok.");
                        if(downloadOver(fileID)){
                            console.log("file download Ok:"+fileID);
                            // TODO FIXME add more logic here
                            setTimeout(function(){ client.destroy(); }, 500);
                            return;
                        }
                        if(fileBlocksDownloadLeft[fileID].length == 0){
                            setTimeout(function(){ client.destroy(); }, 500);
                            return;
                        }
                        // next block download
                        blockID = pop(fileBlocksDownloadLeft[fileID]);
                        client.write(constructHeader(blockID, fileHash, fileSize));
                        console.log("next block:" + blockID);
                    }
                }
            );
        } else {
            chunks.push(data);
            //console.log('DATA: coming...'+typeof(data)+" data:");
        }
    });

    client.on('end', function () {
        console.log('disconnected from server');
    });

    client.on('error', function (err) {
        console.log('error on socket:'+err);
        console.log('fileID:' + fileID + ' remove ower uid:' + owner["uid"] + " ip:" + owner["host"]);
        removeItemOfArray(downloadActiveQueue2[fileID], owner);
        removeItemOfArray(fileBlocksDownloading[fileID], blockID);
        fileBlocksDownloadLeft[fileID].unshift(blockID);
        /* // TODO FIXME
        if (!downloadFailed(fileID)) {
            downloadBlockFromOwner(fileID);
        }
        */
    });

    // Add a 'close' event handler for the client socket
    client.on('close', function () {
        console.log('Connection closed');
    });
}

function pop(arr) {
    return arr.shift();
}

function removeItemOfArray(arr, item) {
    for (var i = arr.length; i--;) {
        if (arr[i] === item) {
            arr.splice(i, 1);
        }
    }
}

//python range
function range(lowEnd,highEnd){
	var arr = [];
	while(lowEnd < highEnd){
	   arr.push(lowEnd++);
	}
	return arr;
}

Buffer.prototype.indexOf = function (needle) {
        if (!(needle instanceof Buffer)) {
                needle = new Buffer(needle + "");
        }
        var length = this.length, needleLength = needle.length, pos = 0,
index;
        for (var i = 0; i < length; ++i) {
                if (needle[pos] === this[i]) {
                        if ((pos + 1) === needleLength) {
                                return index;
                        } else if (pos === 0) {
                                index = i;
                        }
                        ++pos;
                } else if (pos) {
                        pos = 0;
                        i = index;
                }
        }
        return -1;
};

function downloadOver(fileID){
    return (fileBlocksDownloadLeft[fileID].length == 0 && fileBlocksDownloading[fileID].length == 0);
}

downloadFile();
