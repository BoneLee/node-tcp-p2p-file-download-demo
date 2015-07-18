/**
 * Created by bone on 15-7-15.
 */

var net = require('net'),
    fs = require('fs'),
    path = require('path');

var EOF = new Buffer('\r\nEOF\r\n');
var MAX_CONNECTION = 32;
var DEBUG = true;

function startServer(host, port) {
    var clientSockets = [];
    // Create a server instance, and chain the listen function to it
    // The function passed to net.createServer() becomes the event handler for the 'connection' event
    // The sock object the callback function receives UNIQUE for each connection
    var server = net.createServer(function (sock) {
        // We have a connection - a socket object is assigned to the connection automatically
        if (DEBUG) console.log('Connection in: ' + sock.remoteAddress + ':' + sock.remotePort);
        clientSockets.push(sock);
        if (clientSockets.length > MAX_CONNECTION) {
            if (DEBUG) console.log("Exceed max connection count. Drop it.");
            cleanUpSocket(sock);
            return;
        }

        // Add a 'data' event handler to this instance of socket
        sock.on('data', function (data) {
            //if(DEBUG) console.log('DATA ' + sock.remoteAddress + ' length: ' + data.length + "=>"+data);
            try{
                var json = JSON.parse(data);
                var start = json["start"];
                var end = json["end"];
                var fileHash = json["hash"];
            } catch(e){
                if(DEBUG) console.log('http download from. abort it');
                write2socket(sock, "NO SUPPORTED PROTOCOL", null);
                cleanUpSocket(sock);
                return;
            }
            if (start > end) {
                if (DEBUG) console.log('warning block start > end, clean it.');
                cleanUpSocket(sock);
                return;
            }
            getFilePathFromHash(fileHash, function (err, filePath) {
                if (err || !filePath) {
                    if (DEBUG) console.log("File not found in local DB:" + fileHash);
                    cleanUpSocket(sock);
                    return;
                }
                var fileName = path.basename(filePath);
                if (DEBUG) console.log("file: " + fileName + " hash:" + fileHash);
                // TODO FIXME
                //filePath = utils.fbtNormalize(filePath);
                var exists = fs.existsSync(filePath);
                if (!exists) {
                    if (DEBUG) console.log("File not exist:" + fileHash);
                    cleanUpSocket(sock);
                    return;
                }
                fs.stat(filePath, function fileSize(err, stats) {
                    if (err) {
                        if (DEBUG) console.log("File stat err:" + fileHash + " err:" + err);
                        cleanUpSocket(sock);
                        return;
                    }
                    var totalSize = stats.size;
                    if (end >= totalSize) {
                        if (DEBUG) console.log('warning block end>total file size, clean it.');
                        cleanUpSocket(sock);
                        return;
                    }
                    var readStream = fs.createReadStream(filePath, {start: start, end: end, autoClose: true});
                    if (DEBUG) console.log('block start:' + start + " end:" + end + " =" + (end - start + 1));
                    readStream.on('data', function (data) {
                        //console.log("data length: "+data.length);
                        write2socket(sock, data, readStream);
                    });
                    readStream.on('end', function () {
                        if (DEBUG) console.log('stream block EOF.');
                        write2socket(sock, EOF, null);
                    });
                    readStream.on('error', function () {
                        if (DEBUG) console.log('stream data err.');
                        cleanUpSocket(sock);
                    });
                    sock.on('drain', function () {
                        // Resume the read stream when the write stream gets hungry
                        readStream.resume();
                    });
                });
            });
        });

        // Add a 'close' event handler to this instance of socket
        sock.on('close', function (data) {
            cleanUpSocket(sock);
        });
    });
    server.listen(port, host);

    server.on('listening', function (err) {
        if(DEBUG) console.log('Server listening on ' + host +':'+ port);
    });

    /*
    server.close(function () {
        if(DEBUG) console.log("Http server has stopped for host:" + host + " port:" + port);
        // TODO restart
    });
    */

    function cleanUpSocket(sock) {
        if (DEBUG) {
            if(sock.remoteAddress && sock.remotePort)
                console.log('clean up socket: ' + sock.remoteAddress + ' ' + sock.remotePort);
        }
        sock.destroy();
        removeItemOfArray(clientSockets, sock);
    }

    function removeItemOfArray(arr, item) {
        for (var i = arr.length; i--;) {
            if (arr[i] === item) {
                arr.splice(i, 1);
            }
        }
    }

    function getFilePathFromHash(hash, callback) {
        //var filePath = path.join(__dirname, 'mocha.css');
        //var filePath = path.join(__dirname, 'test.js');
        //var filePath = path.join(__dirname, 'course.txt');
        var filePath = path.join(__dirname, 'fill.rar');
        callback(null, filePath);
        // TODO fixme
        /*
         resourceDB.findOne({'verify': parseInt(hash)}, function (err, doc) {
         if (err || doc === null) {
         callback(err);
         }
         else {
         callback(null, doc.path);
         }
         });
         */
    }

    function write2socket(sock, data, readStream) {
        if (sock.writable) {
            var flushed = sock.write(data);
            // Pause the read stream when the write stream gets saturated
            if (!flushed && readStream)
                readStream.pause();
        } else {
            cleanUpSocket(sock);
        }
    }
}
function pipeFileStream(fileStream, socket) {
    //fix memory leak bug
    /*
     http://grokbase.com/t/gg/nodejs/138kjyv011/memory-leak-on-http-chunked-fs-createreadstream-pipe-res
     https://groups.google.com/forum/#!topic/nodejs/A8wbaaPmmBQ
     https://groups.google.com/forum/#!topic/nodejs/wtmIzV0lh8o
     */
    fileStream.on('close', function () {
        socket.destroy.bind(socket);
    })
        .on('error', function () {
            socket.destroy.bind(socket);
        })
        .pipe(socket)
        .on('close', fileStream.destroy.bind(fileStream))
        .on('error', fileStream.destroy.bind(fileStream));
}


function test(){
    startServer('127.0.0.1', 8885);
    startServer('::1', 8885);
    //startServer('192.168.0.108', 8885); // your local LAN IP
}

test();
