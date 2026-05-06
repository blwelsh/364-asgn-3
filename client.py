import socket
import sys
import struct
import datetime
import threading
import time

# initialize socket
soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

host = "localhost"
port = 5000

try:
    soc.bind((host, port))
except:
    print("Bind failed. Error : " + str(sys.exc_info()))
    sys.exit()


# read 1MB file


# congestion control mechanisms

# slow start:

# def congestion_control()

# congestion avoidance:

# fast retransmit

# send each chunk over UDP, with sequence number and checksum in the packet header

# dictionary that maps unreceived ACKs to time sent
unreceived_acks = {}

def send_chunk(chunk, sequence_num, checksum):
    # 4 byte sequence num
    # 2 byte checksum
    # 1024 byte data or less
    # TODO: if last chunk, shouldn't be 1024 bytes

    message = struct.pack("ih1024s", sequence_num, checksum, chunk)

    soc.sendto(message, (host, port))
    return

# handle timeouts:

timeout = 10

# for chunks that need to be retransmitted
retransmit = []

# thread for handling server responses



with open("gistfile1.txt", "rb") as file:

    def handle_ack():
        while True:
            # receieve ACKs
            # TODO: how big should server response be
            ack, address = soc.recvfrom(4)

            if not isinstance(ack, int):
                print("ack not an int... :(")
                continue

            ack_time = datetime.datetime.now()
            if (ack_time - unreceived_acks[ack - 1]).total_seconds() < timeout:
                retransmit.append(ack - 1)
                pass

            # we've received the ACK and no longer need to worry about the chunk. yay!
            unreceived_acks.pop(ack - 1)


    def handle_timeout():
        while True:
            for chunk_num, time_sent in unreceived_acks:
                curr = datetime.datetime.now()

                if (curr - time_sent).total_seconds() < timeout:
                    retransmit.append(chunk_num)

            time.sleep(1)

    chunks = []
    while True:
        chunk = file.read(1024)
        if not chunk:
            break
        chunks.append(chunk)

    total_chunks = len(chunks)

    # congestion control
    remaining_packets = len(chunks)
    cwnd = 1


    base_chunk = 0
    while (remaining_packets > 0):
        # send a chunk for each cwnd
        for i in range(cwnd):
            if i + base_chunk >= total_chunks:
                break

            send_chunk(chunks[i + base_chunk], chunks[i + base_chunk], 0)
            unreceived_acks[i+base_chunk] = datetime.datetime.now()

            print("Sent chunk number " + str(base_chunk + i))




        print("command window = " + str(cwnd))
        base_chunk += cwnd
        remaining_packets = remaining_packets-cwnd

        cwnd+=1

