#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    bpm_ = std::exchange(that.bpm_, nullptr);
    page_ = std::exchange(that.page_, nullptr);
    is_dirty_ = std::exchange(that.is_dirty_, false);
    origin_ = std::exchange( that.origin_ , false);
}

void BasicPageGuard::Drop() {
  if (!origin_) {
    return;
  }
  if(bpm_&&page_ ) bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  origin_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // Drop();
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ =that.is_dirty_;
    origin_ = std::exchange( that.origin_ , false);

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  origin_ = false;
  return {bpm_, page_};
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  origin_ = false;
  return {bpm_, page_};
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) { page->RLatch(); }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // Drop();
 //guard_ = that.guard_;
  guard_.bpm_ = std::exchange(that.guard_.bpm_, nullptr);
  guard_.page_ = std::exchange(that.guard_.page_, nullptr);
  guard_.is_dirty_ = std::exchange(that.guard_.is_dirty_, false);
  guard_.origin_ = std::exchange( that.guard_.origin_, false);
  return *this;
} 

void ReadPageGuard::Drop() {
  if (guard_.origin_) {
    if(guard_.page_) guard_.page_->RUnlatch();
    guard_.Drop();
  }
}
ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) { page->WLatch(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  // Drop();
  //guard_ = that.guard_;
  guard_.bpm_ = std::exchange(that.guard_.bpm_, nullptr);
  guard_.page_ = std::exchange(that.guard_.page_, nullptr);
  guard_.is_dirty_ = std::exchange(that.guard_.is_dirty_, false);
  guard_.origin_ = std::exchange(that.guard_.origin_, false);

  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.origin_) {
    if(guard_.page_) guard_.page_->WUnlatch();
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
