/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id$
 *
 * Authors:
 *   duolong <duolong@taobao.com>
 *
 */

#ifndef TAIR_PACKET_STREAMER_H
#define TAIR_PACKET_STREAMER_H

#include <tbnet.h>

namespace tair {

class tair_packet_streamer : public tbnet::IPacketStreamer {

public:
    /*
     * ���캯��
     */
    tair_packet_streamer();

    /*
     * ���캯��
     */
    tair_packet_streamer(tbnet::IPacketFactory *factory);

    /*
     * ���캯��
     */
    ~tair_packet_streamer();

    /**
     * ����IPacketFactory
     */
    void setPacketFactory(tbnet::IPacketFactory *factory);

    /*
     * �õ���ͷ��Ϣ
     *
     * @param input  Դbuffer
     * @param header ���header
     * @return �Ƿ�ɹ�
     */
    bool getPacketInfo(tbnet::DataBuffer *input, tbnet::PacketHeader *header, bool *broken);

    /*
     * �԰��Ľ���
     *
     * @param input
     * @param header
     * @return ���������ݰ�
     */
    tbnet::Packet *decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header);

    /*
     * ��Packet����װ
     *
     * @param packet ���ݰ�
     * @param output ��װ���������
     * @return �Ƿ�ɹ�
     */
    bool encode(tbnet::Packet *packet, tbnet::DataBuffer *output);

    /*
     * ����packet��flag
     */
    void setPacketFlag(int flag);

public:
    int packet_flag;
};

}

#endif /*TAIR_PACKET_STREAMER_H*/
