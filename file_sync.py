import os
import socket                  
import re
import json
import hashlib
import sys
import getopt
from os.path import isfile, join
import threading
from socket import error as socket_error
import time

PACKET_SIZE = 4096
IPV4_RE = r'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'
PORT = 60001                    

TYPE_ACK = 'TYPE_ACK'
TYPE_INIT = 'TYPE_INIT'
TYPE_DOWNLOAD_PORT_READY = 'TYPE_DOWNLOAD_PORT_READY'

SHARED_FOLDER = None
IP_ADDR = None

def usage():
    print "usage: " + sys.argv[0] + " -p optional-peer-ip -s shared-folder-location"

input_file_b = input_file_t = output_file = None
try:
    opts, args = getopt.getopt(sys.argv[1:], 'p:s:')
except getopt.GetoptError, err:
    usage()
    sys.exit(2)
for o, a in opts:
    if o == '-p':
        IP_ADDR = a
    elif o == '-s':
        SHARED_FOLDER = a
    else:
        assert False, "unhandled option"
if SHARED_FOLDER == None:
    usage()
    sys.exit(2)

def is_valid_ip(ip_addr):
    return ip_addr and (ip_addr == 'localhost' or not not re.match(IPV4_RE, ip_addr))


def receive_socket_data(socket):
    data_buffer = ''
    while True:
        data = socket.recv(PACKET_SIZE)
        if not data:
            try:
                return json.loads(data_buffer)
            except ValueError:
                return None
        data_buffer += data
        try:
            return json.loads(data_buffer)
        except ValueError:        
            pass


def to_filename(obj):
    return obj['filename']

def convert_to_filenames(file_list):
    return map(to_filename, file_list)

def hash_list(aList):
    aList = convert_to_filenames(aList)
    return hashlib.md5(','.join(aList)).hexdigest()

def is_file_in_list(filename, file_list):
    for f in file_list:
        if f['filename'] == filename:
            return True
    return False

def get_missing_files(a, b):
    """
    Returns a list of files which a is missing from b
    """
    missing_files = []
    for f in b:
        if not is_file_in_list(f['filename'], a):
            missing_files.append(f)
    return missing_files


def upload_files(files, socket):
    for fileObj in files:
        filename = fileObj['filename']        
        # Send file
        f = open(SHARED_FOLDER + filename,'r')
        data = f.read(PACKET_SIZE)
        while (data):
           socket.send(data)
           data = f.read(PACKET_SIZE)
        f.close()
        print 'Sent file {}'.format(filename)

        # wait for acknowledgement
        if receive_ACK(socket, filename):
            print 'Peer received {}'.format(filename)
        else:
            print 'Did not receive'


def download_files(files, socket):
    for fileObj in files:
        filename = fileObj['filename']
        filesize = fileObj['bytes']
        bytes_received = 0
        f = open(SHARED_FOLDER + filename, 'a')
        while True:
            data = socket.recv(PACKET_SIZE)      
            bytes_received += sys.getsizeof(data)      
            if data:
                f.write(data)
            if not data or bytes_received >= filesize:
                break
        f.close()
        send_ACK(socket, filename)
        print 'Downloaded {}'.format(filename)



def receive_ACK(socket, payload):
    msg = receive_socket_data(socket)
    return msg and msg['type'] == TYPE_ACK and msg['hash'] == payload


def send_ACK(socket, payload):
    socket.send(json.dumps({'type': TYPE_ACK, 'hash': payload}))


def add_size(f):
    statinfo = os.stat(SHARED_FOLDER + f)
    return {'filename': f, 'bytes': statinfo.st_size}

def get_current_files():
    current_files = [f for f in os.listdir(SHARED_FOLDER) if isfile(join(SHARED_FOLDER, f))]
    return map(add_size, current_files)


def exchange_file_list(socket, current_files):
    # Send file list 
    socket.send(json.dumps({'type': TYPE_INIT, 'files': current_files}))

    # Receive file list
    msg = receive_socket_data(socket)
    peer_files = msg['files']

    received_list_hash = hash_list(peer_files)

    # Acknowledge received list
    send_ACK(socket, received_list_hash)

    # Wait for acknowledgement of its own list sent out
    sent_list_hash = hash_list(current_files)
    if receive_ACK(socket, sent_list_hash):
        print 'File list exchanged'
        return peer_files
    else:
        raise AssertionError('Error receiving ACK')

class Downloader(threading.Thread):
    def __init__(self, to_download, control_socket):
        threading.Thread.__init__(self)
        self.to_download = to_download
        self.control_socket = control_socket

    def run(self):
        s = socket.socket()             
        host = socket.gethostname()    
        s.bind((host, 0))          
        s.listen(1)      
        port = s.getsockname()[1]
        self.control_socket.send(json.dumps({'type': TYPE_DOWNLOAD_PORT_READY, 'port': port}))

        conn, addr = s.accept()

        print 'Got download connection from {}'.format(addr)
        download_files(self.to_download, conn)
        print 'Finished downloading'



class Uploader(threading.Thread):
    def __init__(self, to_upload, control_socket):
        threading.Thread.__init__(self)
        self.to_upload = to_upload
        self.control_socket = control_socket
        
    def run(self):
        s = socket.socket()             
        host = socket.gethostname()
        msg = receive_socket_data(self.control_socket) 
        if msg and msg['type'] == TYPE_DOWNLOAD_PORT_READY:            
            s.connect((host, msg['port']))

            print 'Got upload connection to {}: {}\n'.format(host, msg['port'])
            upload_files(self.to_upload, s)
            print 'Finished uploading'
        else:
            print 'Something went wrong'


def start_sync(current_files, peer_files, socket):
    print 'start sync'
    to_upload = get_missing_files(peer_files, current_files)
    to_download = get_missing_files(current_files, peer_files)

    if len(to_upload) == 0 and len(to_download) == 0:
        print 'Already synchronized.'
        return

    if len(to_upload) > 0:
        uploader = Uploader(to_upload, socket)
        uploader.start()

    if len(to_download) > 0:
        downloader = Downloader(to_download, socket)
        downloader.start()

current_files = get_current_files()

if is_valid_ip(IP_ADDR):
    # client
    print 'CLIENT. Will connect to a peer.'
    s = socket.socket()             
    host = socket.gethostname()     
    try:
        s.connect((host, PORT))
        print 'Connected to peer'
    except socket_error as serr:
        print '{}:{} is not open. Please check the connection.'.format(IP_ADDR, PORT)
        sys.exit(2)

    peer_files = exchange_file_list(s, current_files)    
    start_sync(current_files, peer_files, s)
else:    
    # server
    print 'SERVER. Will wait for a peer.'
    s = socket.socket()             
    host = socket.gethostname()    
    s.bind((host, PORT))          
    s.listen(1)                    
    conn, addr = s.accept()
    print 'Got connection from {}'.format(addr)

    peer_files = exchange_file_list(conn, current_files)
    start_sync(current_files, peer_files, conn)
