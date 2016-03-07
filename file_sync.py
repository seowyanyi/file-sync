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
MODE = None

def usage():
    print "usage: " + sys.argv[0] + " -p ip-address-of-server -m mode -s shared-folder-location\n"

def is_valid_ip(ip_addr):
    return ip_addr and (ip_addr == 'localhost' or not not re.match(IPV4_RE, ip_addr))

try:
    opts, args = getopt.getopt(sys.argv[1:], 'p:m:s:')
except getopt.GetoptError, err:
    usage()
    sys.exit(2)
for o, a in opts:
    if o == '-p':
        IP_ADDR = a
    elif o == '-m':
        MODE = a        
    elif o == '-s':
        SHARED_FOLDER = a
    else:
        print 'Unknown option "{}"'.format(o)
        usage()
        sys.exit(2)
if SHARED_FOLDER == None or (MODE != 'client' and MODE != 'server') or not is_valid_ip(IP_ADDR):
    usage()
    sys.exit(2)



def receive_socket_data(conn):
    data_buffer = ''
    while True:
        data = conn.recv(PACKET_SIZE)
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


def upload_files(files, conn):
    for fileObj in files:
        filename = fileObj['filename']        
        # Send file
        f = open(SHARED_FOLDER + filename,'r')
        data = f.read(PACKET_SIZE)
        while (data):
           conn.send(data)
           data = f.read(PACKET_SIZE)
        f.close()
        print 'Sent file {}'.format(filename)

        # wait for acknowledgement
        if receive_ACK(conn, filename):
            print 'Peer received {}'.format(filename)
        else:
            print 'Did not receive'


def download_files(files, conn):
    for fileObj in files:
        filename = fileObj['filename']
        filesize = fileObj['bytes']
        bytes_received = 0
        f = open(SHARED_FOLDER + filename, 'a')
        while True:
            data = conn.recv(PACKET_SIZE)      
            bytes_received += sys.getsizeof(data)      
            if data:
                f.write(data)
            if not data or bytes_received >= filesize:
                break
        f.close()
        send_ACK(conn, filename)
        print 'Downloaded {}'.format(filename)



def receive_ACK(conn, payload):
    msg = receive_socket_data(conn)
    return msg and msg['type'] == TYPE_ACK and msg['hash'] == payload


def send_ACK(conn, payload):
    conn.send(json.dumps({'type': TYPE_ACK, 'hash': payload}))


def add_size(f):
    statinfo = os.stat(SHARED_FOLDER + f)
    return {'filename': f, 'bytes': statinfo.st_size}

def get_current_files():
    current_files = [f for f in os.listdir(SHARED_FOLDER) if isfile(join(SHARED_FOLDER, f))]
    return map(add_size, current_files)


def exchange_file_list(conn, current_files):
    # Send file list 
    conn.send(json.dumps({'type': TYPE_INIT, 'files': current_files}))

    # Receive file list
    msg = receive_socket_data(conn)
    peer_files = msg['files']

    received_list_hash = hash_list(peer_files)

    # Acknowledge received list
    send_ACK(conn, received_list_hash)

    # Wait for acknowledgement of its own list sent out
    sent_list_hash = hash_list(current_files)
    if receive_ACK(conn, sent_list_hash):
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
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)             
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
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)             
        host = socket.gethostname()
        msg = receive_socket_data(self.control_socket) 
        if msg and msg['type'] == TYPE_DOWNLOAD_PORT_READY:            
            s.connect((host, msg['port']))

            print 'Got upload connection to {}: {}'.format(host, msg['port'])
            upload_files(self.to_upload, s)
            print 'Finished uploading'
        else:
            print 'Something went wrong'


def start_sync(current_files, peer_files, conn):
    print 'start sync'
    to_upload = get_missing_files(peer_files, current_files)
    to_download = get_missing_files(current_files, peer_files)

    if len(to_upload) == 0 and len(to_download) == 0:
        print 'Already synchronized.'
        return

    if len(to_upload) > 0:
        uploader = Uploader(to_upload, conn)
        uploader.start()

    if len(to_download) > 0:
        downloader = Downloader(to_download, conn)
        downloader.start()


current_files = get_current_files()
connection = None

if MODE == 'client':
    # client
    print 'CLIENT. Will connect to a peer.'
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)             
    try:
        connection.connect((IP_ADDR, PORT))
        print 'Connected to peer'
    except socket_error as serr:
        print '{}:{} is not open. Please check the connection.'.format(IP_ADDR, PORT)
        sys.exit(2)

else:    
    # server
    print 'SERVER. Will wait for a peer.'
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)             
    s.bind((IP_ADDR, PORT))
    s.listen(1)                    
    connection, addr = s.accept()
    print 'Got connection from {}'.format(addr)


peer_files = exchange_file_list(connection, current_files)    
start_sync(current_files, peer_files, connection)
