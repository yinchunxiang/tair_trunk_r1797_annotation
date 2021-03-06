/*
 * (C) 2007-2010 Alibaba Group Holding Limited
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: response_mreturn_packet.hpp 28 2010-09-19 05:18:09Z ganyu.hfl@taobao.com $
 *
 * Authors:
 *   ganyu.hfl <ganyu.hfl@taobao.com>
 *     - initial release
 *
 */
#ifndef RESPONSE_MRETURN_PACKET_H
#define RESPONSE_MRETURN_PACKET_H
#include "base_packet.hpp"
#include "data_entry.hpp"

namespace tair {
  using namespace common;
  class response_mreturn : public base_packet {
  public:
    response_mreturn()
    {
      setPCode(TAIR_RESP_MRETURN_PACKET);
      code = 0;
      config_version = 0;
      msg[0] = '\0';
      key_count = 0;
      key_code_map = NULL;
    }

    ~response_mreturn()
    {
      if (key_code_map != 0) {
        key_code_map_t::iterator it = key_code_map->begin();
        while (it != key_code_map->end()) {
          data_entry *key = it->first;
          ++it;
          delete key;
        }
        delete key_code_map;
      }
    }

    bool encode(tbnet::DataBuffer *output)
    {
      output->writeInt32(config_version);
      output->writeInt32(code);
      output->writeString(msg);
      output->writeInt32(key_count);
      if (key_count > 0) {
        key_code_map_t::iterator it = key_code_map->begin();
        while (it != key_code_map->end()) {
          data_entry *key = it->first;
          key->encode(output);
          output->writeInt32(it->second);
          ++it;
        }
      }
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header)
    {
      if (header->_dataLen < 12) {
        return false;
      }
      config_version = input->readInt32();
      code = input->readInt32();
      char *p = msg;
      input->readString(p, sizeof(msg));

      uint32_t key_count = input->readInt32();
      if (key_count > 0) {
        key_code_map = new key_code_map_t;
        for (uint32_t i = 0; i < key_count; ++i) {
          data_entry *key = new data_entry();
          key->decode(input);
          int rc = input->readInt32();
          key_code_map->insert(make_pair(key, rc));
        }
      }
      return true;
    }

    void add_key_code(data_entry *key, int code, bool copy = false)
    {
      if (key == NULL) {
        return ;
      }
      if (key_code_map == NULL) {
        key_code_map = new key_code_map_t;
      }
      if (copy) {
        key = new data_entry(*key);
      }
      key_code_map->insert(make_pair(key, code));
      ++key_count;
    }

    void set_code(int code)
    {
      this->code = code;
    }

    int get_code() const
    {
      return this->code;
    }

    void set_message(const char *msg)
    {
      snprintf(this->msg, sizeof(msg), "%s", msg);
    }

    char* get_message()
    {
      return msg;
    }

    void swap(response_mreturn &rhs) {
      std::swap(code, rhs.code);
      std::swap(config_version, rhs.config_version);
      std::swap(key_count, rhs.key_count);
      std::swap(key_code_map, rhs.key_code_map);
    }

    //@override
    virtual size_t size() {
      size_t sz = 0;
      if (msg[0]) {
        sz += strlen(msg);
      }
      if (key_code_map != NULL) {
        key_code_map_t::const_iterator itr = key_code_map->begin();
        while (itr != key_code_map->end()) {
          sz += itr->first->encoded_size() + sizeof(itr->second);
          ++itr;
        }
      }
      return sz;
    }
  public:
    int code;
    uint32_t config_version;
    char msg[128];
    int key_count;
    key_code_map_t *key_code_map;
  private:
    response_mreturn(const response_mreturn&);
    response_mreturn& operator=(const response_mreturn&);
  };
  /*
   * class response_mreturn_dup, used in duplicatation only
   */
  class response_mreturn_dup : public response_mreturn {
  public:
    response_mreturn_dup() {
      setPCode(TAIR_RESP_MRETURN_DUP_PACKET);
      server_id = 0;
      packet_id = 0;
      bucket_id = 0;
    }
    ~response_mreturn_dup() { }

    bool encode(tbnet::DataBuffer *output) {
      if (!response_mreturn::encode(output)) {
        return false;
      }
      output->writeInt32(bucket_id);
      output->writeInt32(packet_id);
      output->writeInt64(server_id);
      return true;
    }

    bool decode(tbnet::DataBuffer *input, tbnet::PacketHeader *header) {
      if (!response_mreturn::decode(input, header)) {
        return false;
      }
      bucket_id = input->readInt32();
      packet_id = input->readInt32();
      server_id = input->readInt64();
      return true;
    }

  public:
    uint64_t server_id;
    uint32_t packet_id;
    int32_t  bucket_id;
  };
}

#endif
