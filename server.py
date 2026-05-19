import socket
import sys
import struct
import random
import time

host = "localhost"
port = 5000


soc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

try:
    soc.bind((host, port))
except:
    print("Bind failed. Error : " + str(sys.exc_info()))
    sys.exit()


def calc_checksum(databytes):
    sum = 0
    for byte in databytes:
        sum += byte
    res = sum % 65535
    return res


# for detecting first part of packet
expected_seq = 0
batch_start_time = None
ack_pending = False

soc.settimeout(0.01)


while True:
    # note: should receieve 1032 bytes

    header_format = "IH1024s"
    packet_size = struct.calcsize(header_format)

    try:
        data, address = soc.recvfrom(packet_size)
    except socket.timeout:
        data = None

    if data is not None:
        # print(data)
        unpacked = struct.unpack("IH1024s", data)

        sequence_num = unpacked[0]
        checksum = unpacked[1]
        chunk = unpacked[2]

        # 1. Validate checksum
        if not (calc_checksum(chunk) == checksum):
            print("discarding packet due to faulty checksum")
            continue

        # simulate packet loss
        loss = random.randint(1, 10)
        if loss == 1:
            # dropping packet :(
            print("Dropping packet: " + str(sequence_num))
            continue


        # first packet of window
        if (sequence_num == expected_seq):
            # 4. write chunk to output file
            with open("output.txt", "ab") as f:
                f.write(chunk.rstrip(b"\x00"))

            expected_seq += 1

            if not ack_pending:
                batch_start_time = time.time()
                ack_pending = True
        else:
            print("out of order packet - packet receieved: " + str(sequence_num) + " expected: " + str(expected_seq))
            message = struct.pack("i", expected_seq)
            soc.sendto(message, address)


        # checking to see if it's the first packet in window and then applying RTT
    if ack_pending and time.time() - batch_start_time >= 0.1:
        # 2. Send ACKs back to client, containing sequence_num + 1
        message = struct.pack("i", expected_seq)
        soc.sendto(message, address)
        ack_pending = False
