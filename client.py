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

# read 1MB file

def calc_checksum(databytes):
    sum = 0
    for byte in databytes:
        sum += byte
    res = sum % 65535
    return res

# time of most recently received ACK
recent_ack_time = datetime.datetime.now()
recent_ack = 0

def send_chunk(chunk, sequence_num, checksum):
    # 4 byte sequence num
    # 2 byte checksum
    # 1024 byte data or less

    message = struct.pack("iH1024s", sequence_num, checksum, chunk)

    soc.sendto(message, (host, port))
    return

# handle timeouts:
timeout = 0.5

# for chunks that need to be retransmitted

send_base = 0
next_to_send = 0
cwnd = 1

# ssthresh starts at infinity
ssthresh = float("inf")

# for fast retransmit
dup_ack_count = 0

# last ACK num received
last_ack = 0


# thread for receiving acks from server
def handle_ack():
    global send_base, cwnd, ssthresh, dup_ack_count, last_ack, next_to_send, recent_ack_time, recent_ack

    while True:
        # receieve ACKs
        message, address = soc.recvfrom(4)

        ack = struct.unpack("i", message)[0]


        if not isinstance(ack, int):
            print("error on type of ACK received")
            continue

        if ack > send_base:
            recent_ack_time = datetime.datetime.now()
            recent_ack = ack
            new_ack = ack - send_base

            send_base = ack
            last_ack = ack
            dup_ack_count = 0

            # slow start:
            if cwnd < ssthresh:
                cwnd += new_ack
            # congestion avoidance:
            else:
                cwnd += new_ack / cwnd


        # duplicate ack/fast retransmit
        elif ack == last_ack:
            dup_ack_count += 1

            if dup_ack_count >= 3:
                dup_ack_count = 0
                missing_packet = ack
                print("Fast retransmit on packet " + str(missing_packet))

                ssthresh = max(cwnd / 2, 1)
                cwnd = ssthresh + 3

                send_chunk(chunks[ack], ack, calc_checksum(chunks[ack]))


def handle_timeout():
    global cwnd, ssthresh, next_to_send, recent_ack_time

    while True:
        now = datetime.datetime.now()

        if (now - recent_ack_time).total_seconds() > timeout:
            ssthresh = max(cwnd / 2, 1)
            cwnd = 1

            recent_ack_time = datetime.datetime.now()

            if send_base < total_chunks:
                print("Timeout. retransmitting packet #" + str(send_base))
                send_chunk(chunks[send_base], send_base, calc_checksum(chunks[send_base]))

            next_to_send = send_base

        time.sleep(0.1)

with open("gistfile1.txt", "rb") as file:


    chunks = []
    ack_thread = threading.Thread(target=handle_ack, daemon=True)
    timeout_thread = threading.Thread(target=handle_timeout, daemon=True)
    ack_thread.start()
    timeout_thread.start()


    while True:
        chunk = file.read(1024)
        if not chunk:
            break
        chunks.append(chunk)

    total_chunks = len(chunks)

    # congestion control
    remaining_packets = len(chunks)

    base_chunk = 0
    seq = None
    while (send_base < total_chunks):
        packets_to_send = []

        # send a chunk for each cwnd
        # int(cwnd) because it can be a float
        while next_to_send < total_chunks and next_to_send < send_base + int(cwnd):
            # unreceived_acks[next_to_send] = datetime.datetime.now()
            seq = next_to_send
            next_to_send += 1
            packets_to_send.append(seq)

        for seq in packets_to_send:
            send_chunk(chunks[seq], seq, calc_checksum(chunks[seq]))
            print("Sent chunk number " + str(seq))

        time.sleep(0.01)


