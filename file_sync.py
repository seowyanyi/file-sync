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
import select
import errno

PACKET_SIZE = 4096
IPV4_RE = r'^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$'
CONTROL_PORT = 60002                    
FILE_TRANSFER_PORT = 50002

TYPE_ACK = 'TYPE_ACK'
TYPE_INIT = 'TYPE_INIT'
TYPE_PORT_READY = 'TYPE_PORT_READY'

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
else:
    if SHARED_FOLDER[len(SHARED_FOLDER) - 1] != '/':
        SHARED_FOLDER += '/'


def receive_socket_data(conn):
    """
    Receives socket data in json format.
    Parses the JSON and returns the deserialized python object.
    """
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



def receive_ACK(conn, payload):
    msg = receive_socket_data(conn)
    return msg and msg['type'] == TYPE_ACK and msg['hash'] == payload


def send_ACK(conn, payload):
    conn.send(json.dumps({'type': TYPE_ACK, 'hash': payload}))


def add_size(filename):
    f = open(SHARED_FOLDER + filename,'r')
    data = f.read(PACKET_SIZE)
    bytes_to_send = sys.getsizeof(data)    
    while data:
        data = f.read(PACKET_SIZE)
        if data:
            bytes_to_send += sys.getsizeof(data)

    f.close()
    return {'filename': filename, 'bytes': bytes_to_send}

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
    def __init__(self, to_download, file_socket, control_socket):
        threading.Thread.__init__(self)
        self.to_download = to_download
        self.control_socket = control_socket
        self.file_socket = file_socket

    def download_files(self, files):
        self.file = None
        self.file_in_progress = None

        for fileObj in files:
            filename = fileObj['filename']
            filesize = fileObj['bytes']
            bytes_received = 0

            f = open(SHARED_FOLDER + filename, 'a')
            self.file = f
            self.file_in_progress = filename
            while True:
                data = self.file_socket.recv(PACKET_SIZE)      
                bytes_received += sys.getsizeof(data)      

                if data:
                    f.write(data)
                if not data or bytes_received >= filesize:
                    break
            
            f.close()
            if bytes_received >= filesize:
                self.file = None            
                self.file_in_progress = None
                send_ACK(self.control_socket, filename)
                print 'Downloaded {}...................... {} %'.format(filename, float(bytes_received)/filesize*100)                    

            else:
                print 'Error. Received {} bytes out of {} bytes'.format(bytes_received, filesize)
                self.file.close()
                os.remove(SHARED_FOLDER + self.file_in_progress)
                print 'Deleted incomplete download {}'.format(self.file_in_progress)

    def run(self):
        try:        
            self.download_files(self.to_download)
            print 'Finished downloading'
        except socket_error, e:
            if isinstance(e.args, tuple):
                if e[0] == errno.EPIPE:
                   # remote peer disconnected
                   print "Downloader: Detected remote disconnect"
                elif e[0] == "timed out":
                    print 'Downloader: Connection timed out.'
                else:
                    print "errno is {}".format(e[0])                    
            else:
                print "socket error ", e      

            # Delete incomplete downloads
            print 'Incomplete: {}'.format(self.file_in_progress)
            if self.file_in_progress and self.file:
                self.file.close()
                os.remove(SHARED_FOLDER + self.file_in_progress)
                print 'Deleted incomplete download {}'.format(self.file_in_progress)



class Uploader(threading.Thread):
    def __init__(self, to_upload, file_socket, control_socket):
        threading.Thread.__init__(self)
        self.to_upload = to_upload
        self.control_socket = control_socket        
        self.file_socket = file_socket
        
    def upload_files(self, files):
        for fileObj in files:
            filename = fileObj['filename']        
            # Send file
            f = open(SHARED_FOLDER + filename,'r')
            data = f.read(PACKET_SIZE)

            while data:
               self.file_socket.send(data)
               data = f.read(PACKET_SIZE)

            f.close()
            print 'Uploaded file {}.'.format(filename)

            # wait for acknowledgement
            if receive_ACK(self.control_socket, filename):
                print 'Peer received {}'.format(filename)

    def run(self):
        try:
            self.upload_files(self.to_upload)
            print 'Finished uploading'
        except socket_error, e:
            if isinstance(e.args, tuple):
                if e[0] == errno.EPIPE:
                   # remote peer disconnected
                   print "Uploader: Detected remote disconnect"
                elif e[0] == "timed out":
                    print 'Uploader: Connection timed out.'  
                else:
                    print "errno is {}".format(e[0])

            else:
                print "socket error ", e


def get_connection(port, control_socket=None):
    if MODE == 'client':
        if control_socket:
            # Wait for signal that file port is ready
            msg = receive_socket_data(control_socket)
            if msg['type'] != TYPE_PORT_READY:
                print 'Error: Did not receive port ready signal'
                print msg
                sys.exit(2)

        # client
        print 'CLIENT. Will connect to a peer on port {}.'.format(port)
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)             
        try:
            connection.connect((IP_ADDR, port))
            print 'Connected to peer'
            return connection
        except socket_error as serr:
            print 'Cannot connect to {}:{}. Please check that the server is up.'.format(IP_ADDR, port)
            sys.exit(2)

    else:    
        # server        
        print 'SERVER. Will wait for a peer on port {}.'.format(port)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)             
        s.bind((IP_ADDR, port))
        s.listen(1)
        if control_socket:
            control_socket.send(json.dumps({'type': TYPE_PORT_READY}))

        connection, addr = s.accept()
        print 'Got connection from {}'.format(addr)
        return connection


def start_sync(current_files, peer_files, control_socket):
    to_upload = get_missing_files(peer_files, current_files)
    to_download = get_missing_files(current_files, peer_files)

    if len(to_upload) == 0 and len(to_download) == 0:
        print 'Already synchronized.'
        return

    file_socket = get_connection(FILE_TRANSFER_PORT, control_socket)
    file_socket.settimeout(10.0)

    if len(to_upload) > 0:
        print 'Should upload {}'.format(to_upload)
        uploader = Uploader(to_upload, file_socket, control_socket)
        uploader.start()

    if len(to_download) > 0:
        print 'Should download {}'.format(to_download)        
        downloader = Downloader(to_download, file_socket, control_socket)
        downloader.start()

current_files = get_current_files()
control_socket = get_connection(CONTROL_PORT)
control_socket.settimeout(10.0)
    
try:
    peer_files = exchange_file_list(control_socket, current_files)    
    start_sync(current_files, peer_files, control_socket)
except socket.timeout:
    print 'Connection timed out.'
