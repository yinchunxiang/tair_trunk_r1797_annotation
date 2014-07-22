/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: packet_streamer.cpp 770 2012-04-23 09:30:12Z nayan@taobao.com $
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *
 */

#include "define.hpp"
#include "packet_streamer.hpp"

namespace tair {

/*
 * ���캯��
 */
tair_packet_streamer::tair_packet_streamer() : packet_flag(TAIR_PACKET_FLAG) {}
/*
 * ���캯��
 */
tair_packet_streamer::tair_packet_streamer(tbnet::IPacketFactory *factory)
  : IPacketStreamer(factory), packet_flag(TAIR_PACKET_FLAG) {}

/*
 * ���캯��
 */
tair_packet_streamer::~tair_packet_streamer() {}

/**
 * ����IPacketFactory
 */
void tair_packet_streamer::setPacketFactory(tbnet::IPacketFactory *factory) {
    _factory = factory;
}

/*
 * �õ���ͷ��Ϣ
 *
 * @param input  Դbuffer
 * @param header ���header
 * @return �Ƿ�ɹ�
 */
bool tair_packet_streamer::getPacketInfo(tbnet::DataBuffer *input, tbnet::PacketHeader *header, bool *broken) {
    if (_existPacketHeader) {
        if (input->getDataLen() < (int)(4 * sizeof(int)))
            return false;
        int flag = input->readInt32();
        header->_chid = input->readInt32();
        header->_pcode = input->readInt32();
        header->_dataLen = input->readInt32();
        if (flag != packet_flag || header->_dataLen < 0 ||
                header->_dataLen > 0x4000000) { // 64M
            TBSYS_LOG(ERROR, "stream error: %x<>%x, dataLen: %d", flag, packet_flag, header->_dataLen);
            *broken = true;
        }
    } else if (input->getDataLen() == 0) {
        return false;
    }
    return true;
}

/*
 * �԰��Ľ���
 *
 * @param input
 * @param header
 * @return ���������ݰ�
 */
tbnet::Packet *tair_packet_streamer::decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
    assert(_factory != NULL);
    tbnet::Packet *packet = _factory->createPacket(header->_pcode);
    if (packet != NULL) {
        if (!packet->decode(input, header)) {
            packet->free();
            packet = NULL;
        }
    } else {
        input->drainData(header->_dataLen);
    }
    return packet;
}

/*
 * ��Packet����װ
 *
 * @param packet ���ݰ�
 * @param output ��װ���������
 * @return �Ƿ�ɹ�
 */
bool tair_packet_streamer::encode(tbnet::Packet *packet, tbnet::DataBuffer *output) {
    tbnet::PacketHeader *header = packet->getPacketHeader();

    // Ϊ�˵�encodeʧ�ָܻ�ʱ��
    int oldLen = output->getDataLen();
    // dataLen��λ��
    int dataLenOffset = -1;
    int headerSize = 0;

    // �������ͷ��Ϣ,д��ͷ��Ϣ
    if (_existPacketHeader) {
        output->writeInt32(packet_flag);
        output->writeInt32(header->_chid);
        output->writeInt32(header->_pcode);
        dataLenOffset = output->getDataLen();
        output->writeInt32(0);
        headerSize = 4 * sizeof(int);
    }
    // д����
    if (packet->encode(output) == false) {
        TBSYS_LOG(ERROR, "encode error");
        output->stripData(output->getDataLen() - oldLen);
        return false;
    }
    // ���������
    header->_dataLen = output->getDataLen() - oldLen - headerSize;
    // ���հѳ��Ȼص�buffer��
    if (dataLenOffset >= 0) {
        unsigned char *ptr = (unsigned char *)(output->getData() + dataLenOffset);
        output->fillInt32(ptr, header->_dataLen);
    }

    return true;
}

/*
 * ����packet��flag
 */
void tair_packet_streamer::setPacketFlag(int flag) {
    packet_flag = flag;
}

}

/////////////
