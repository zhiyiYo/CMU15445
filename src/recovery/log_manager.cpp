//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"

namespace bustub {
/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
  if (enable_logging) {
    return;
  }

  enable_logging = true;
  flush_thread_ = new std::thread([&] {
    while (enable_logging) {
      std::unique_lock<std::mutex> lock(latch_);

      // flush log to disk if log time out or log buffer is full
      cv_.wait_for(lock, log_timeout, [&] { return need_flush_.load(); });
      if (log_buffer_offset_ > 0) {
        std::swap(log_buffer_, flush_buffer_);
        std::swap(log_buffer_offset_, flush_buffer_offset_);
        disk_manager_->WriteLog(flush_buffer_, flush_buffer_offset_);
        flush_buffer_offset_ = 0;
        SetPersistentLSN(next_lsn_ - 1);
      }

      need_flush_ = false;
      cv_append_.notify_all();
    }
  });
}

/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
  enable_logging = false;
  Flush();
  flush_thread_->join();
  delete flush_thread_;
  flush_thread_ = nullptr;
}

/**
 * append a log record into log buffer,
 * you MUST set the log record's lsn within this method
 * @return lsn that is assigned to this log record
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
  std::unique_lock<std::mutex> lock(latch_);

  // flush log to disk when the log buffer is full
  if (log_record->size_ + log_buffer_offset_ > LOG_BUFFER_SIZE) {
    // wake up flush thread to write log
    need_flush_ = true;
    cv_.notify_one();

    // block current thread until log buffer is emptied
    cv_append_.wait(lock, [&] { return log_record->size_ + log_buffer_offset_ <= LOG_BUFFER_SIZE; });
  }

  // serialize header
  log_record->lsn_ = next_lsn_++;
  memcpy(log_buffer_ + log_buffer_offset_, log_record, LogRecord::HEADER_SIZE);
  int pos = log_buffer_offset_ + LogRecord::HEADER_SIZE;

  // serialize body
  switch (log_record->GetLogRecordType()) {
    case LogRecordType::INSERT:
      memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
      pos += sizeof(RID);
      log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
      break;

    case LogRecordType::MARKDELETE:
    case LogRecordType::APPLYDELETE:
    case LogRecordType::ROLLBACKDELETE:
      memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(RID));
      pos += sizeof(RID);
      log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
      break;

    case LogRecordType::UPDATE:
      memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
      pos += sizeof(RID);
      log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
      pos += 4 + static_cast<int>(log_record->old_tuple_.GetLength());
      log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
      break;

    case LogRecordType::NEWPAGE:
      memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(page_id_t));
      pos += sizeof(page_id_t);
      memcpy(log_buffer_ + pos, &log_record->page_id_, sizeof(page_id_t));
      break;

    default:
      break;
  }

  // update log buffer offset
  log_buffer_offset_ += log_record->size_;
  return log_record->lsn_;
}

void LogManager::Flush() {
  if (!enable_logging) {
    return;
  }

  std::unique_lock<std::mutex> lock(latch_);
  need_flush_ = true;
  cv_.notify_one();

  // block thread until flush finished
  cv_append_.wait(lock, [&] { return !need_flush_.load(); });
}

}  // namespace bustub
