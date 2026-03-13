#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts <commit_ts_) {
    throw Exception("read ts <commit ts");
  }
  current_reads_[read_ts]++;
  if(read_ts>=watermark_){
    return ;
  }
  watermark_=read_ts;
}

void Watermark::RemoveTxn(timestamp_t read_ts) {
  auto it = current_reads_.find(read_ts);
  if (it == current_reads_.end()) {
    return;
  }
  if (--it->second == 0) {
    current_reads_.erase(it);
    watermark_ = current_reads_.empty() ? commit_ts_ : current_reads_.begin()->first;
  }
}


}  // namespace bustub
