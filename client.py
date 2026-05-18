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

lock = threading.Lock()


# read 1MB file

def calc_checksum(databytes):
    sum = 0
    for byte in databytes:
        sum += byte
    res = sum % 65535
    return res

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

    message = struct.pack("iH1024s", sequence_num, checksum, chunk)

    soc.sendto(message, (host, port))
    return

# handle timeouts:

timeout = 0.5

# for chunks that need to be retransmitted
retransmit = []


with open("gistfile1.txt", "rb") as file:
    # most recently received ACK
    next_sequence = 0


    send_base = 0
    next_to_send = 0
    cwnd = 1
    ssthresh = float("inf")
    dup_ack_count = 0
    last_ack = 0

    # thread for receiving acks from server
    def handle_ack():
        global send_base, cwnd, ssthresh, dup_ack_count, last_ack, next_to_send

        while True:
            # receieve ACKs
            retransmit_packet = None
            message, address = soc.recvfrom(4)

            ack = struct.unpack("i", message)[0]

            with lock:
                print("received new ack :) from", ack)


                if not isinstance(ack, int):
                    print("ack not an int... :(")
                    continue

                if ack > send_base:
                    for seq in range(send_base, ack):
                        unreceived_acks.pop(seq, None)

                    send_base = ack
                    last_ack = ack
                    dup_ack_count = 0

                    #slow start:
                    if cwnd < ssthresh:
                        cwnd += 1
                    # congestion avoidance:
                    else:
                        cwnd += 1 / cwnd

                # duplicate ack/fast retransmit
                elif ack == last_ack:
                    dup_ack_count += 1

                    if dup_ack_count == 3:
                        missing_packet = ack
                        print("Fast retransmit " + str(missing_packet))

                        retransmit_packet = ack
                        ssthresh = max(cwnd/2, 1)
                        cwnd = ssthresh + 3

            if retransmit_packet is not None:
                send_chunk(chunks[retransmit_packet], retransmit_packet, calc_checksum(chunks[retransmit_packet]))

            # we've received the ACK and no longer need to worry about the chunk. yay!



    def handle_timeout():
        global cwnd, ssthresh, next_to_send

        while True:
            to_retransmit = None

            with lock:
                now = datetime.datetime.now()

                if send_base in unreceived_acks:
                    time_sent = unreceived_acks[send_base]

                    if (now - time_sent).total_seconds() > timeout:

                        to_retransmit = send_base

                        unreceived_acks[send_base] = datetime.datetime.now()

                        ssthresh = max(cwnd / 2, 1)
                        cwnd = 1

                        next_to_send = send_base

            if to_retransmit is not None:
                print("Timeout. retransmitting packet" + str(to_retransmit))
                send_chunk(chunks[to_retransmit], to_retransmit, calc_checksum(chunks[to_retransmit]))

            time.sleep(0.1)


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

        with lock:
            # send a chunk for each cwnd
            # int(cwnd) because it can be a float
            while next_to_send < total_chunks and next_to_send < send_base + int(cwnd):
                unreceived_acks[next_to_send] = datetime.datetime.now()
                seq = next_to_send
                next_to_send += 1
                packets_to_send.append(seq)

        for seq in packets_to_send:
            send_chunk(chunks[seq], seq, calc_checksum(chunks[seq]))
            print("Sent chunk number " + str(seq))

        time.sleep(0.01)


