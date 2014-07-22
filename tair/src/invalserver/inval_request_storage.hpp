/*
 * (C) 2007-2012 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: inval_request_storage.hpp 28 2012-08-17 05:18:09Z fengmao.pj@taobao.com $
 *
 * Authors:
 *   fengmao <fengmao.pj@taobao.com>
 *
 */
#ifndef InvalRequestStorage_H
#define InvalRequestStorage_H

#include <tbsys.h>
#include <tbnet.h>

#include "define.hpp"
#include "log.hpp"
#include "base_packet.hpp"
#include <string>
#include "packet_factory.hpp"
namespace tair {
  class InvalRequestStorage : public tbsys::CDefaultRunnable {
  public:
    InvalRequestStorage();

    ~InvalRequestStorage();
  public:
    void setThreadParameter(const std::string& data_path, const std::string& queue_name,
        const float read_disk_threshold, const float write_disk_threshold, const int max_cached_packet_count,
        tair_packet_factory* packet_factory);

    void stop();

    void run(tbsys::CThread *thread, void *arg);

    int get_packet_count();

    int get_packet_count_on_disk();

    inline int get_queue_max_size()
    {
      return max_cached_packet_count;
    }

    inline int get_write_threshold()
    {
      return write_thresh;
    }

    inline int get_read_threshold()
    {
      return read_thresh;
    }

    int write_request(base_packet* packet);

    int read_request(std::vector<base_packet*>& packet_vector, const int read_count);
    std::string get_info();
  private:
    int read_packet_from_disk();

    int write_packet_to_disk(bool write_all = false);

    int decode_packet(base_packet* &packet, tbsys::queue_item *item);

    int encode_packet(tbnet::DataBuffer* &buffer, base_packet* packet);

    int open_disk_queue();
  private:
    tbnet::PacketQueue request_queue;
    tbnet::PacketHeader *packet_header;
    tbnet::DataBuffer* data_buffer;
    tair_packet_factory *packet_factory;

    int max_cached_packet_count;
    static const uint32_t DATA_BUFFER_SIZE = 8192;//8k
    static const uint32_t DATA_PACKET_MIN_SIZE = 7;
    //the thread that writes/reads packet to/from the disk, should not hold the lock for a long time.

    //if queue's size > write_disk_threshold * max_cached_packet_count, write data to the file.
    float write_disk_threshold;
    int write_thresh;
    //if queue's size < read_disk_threshold * max_cached_packet_count, read data from the file.
    float read_disk_threshold;
    int read_thresh;
    tbsys::CThreadCond queue_cond;
    tbsys::CFileQueue* disk_queue;
    enum { RETURN_SUCCESS = 0, RETURN_FAILED = 1 };
    std::string queue_name;
    std::string data_path;
    //for supporting debug
    int32_t packet_count_on_disk;
  };
}
#endif
